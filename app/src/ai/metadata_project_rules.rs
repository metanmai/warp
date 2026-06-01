use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use ai::project_context::model::{ProjectContextModel, ProjectRule};
use remote_server::proto::{
    file_context_proto, FileContextProto, ReadFileContextFile, ReadFileContextRequest,
};
use repo_metadata::local_model::{GetContentsArgs, IndexedRepoState};
use repo_metadata::{RepoContent, RepoMetadataModel, RepositoryCoverage, RepositoryIdentifier};
use warp_util::local_or_remote_path::LocalOrRemotePath;
use warp_util::remote_path::RemotePath;
use warpui::{AppContext, Entity, ModelContext, SingletonEntity};

use crate::remote_server::manager::RemoteServerManager;

pub(crate) struct MetadataProjectRulesModel {
    refresh_generations: HashMap<RepositoryIdentifier, u64>,
    next_refresh_generation: u64,
}
type ProjectRuleContentsFuture =
    Pin<Box<dyn Future<Output = anyhow::Result<Vec<(LocalOrRemotePath, String)>>> + Send>>;

impl MetadataProjectRulesModel {
    pub(crate) fn new(ctx: &mut ModelContext<Self>) -> Self {
        let repo_metadata = RepoMetadataModel::handle(ctx);
        ctx.subscribe_to_model(&repo_metadata, |me, event, ctx| {
            me.handle_repo_metadata_event(event, ctx);
        });

        let repo_metadata = RepoMetadataModel::as_ref(ctx);
        let mut repo_ids = repo_metadata.local_repository_ids(ctx);
        repo_ids.extend(
            repo_metadata
                .remote_repository_ids(ctx)
                .cloned()
                .map(RepositoryIdentifier::Remote),
        );
        let mut model = Self {
            refresh_generations: HashMap::new(),
            next_refresh_generation: 0,
        };
        for repo_id in repo_ids {
            model.refresh_or_fallback_project_rules_for_repo(&repo_id, ctx);
        }
        model
    }

    fn handle_repo_metadata_event(
        &mut self,
        event: &repo_metadata::wrapper_model::RepoMetadataEvent,
        ctx: &mut ModelContext<Self>,
    ) {
        use repo_metadata::wrapper_model::RepoMetadataEvent;

        match event {
            RepoMetadataEvent::RepositoryUpdated { id: repo_id }
            | RepoMetadataEvent::FileTreeEntryUpdated { id: repo_id } => {
                self.refresh_or_fallback_project_rules_for_repo(repo_id, ctx);
            }
            RepoMetadataEvent::FileTreeUpdated { ids } => {
                for repo_id in ids
                    .iter()
                    .filter(|repo_id| matches!(repo_id, RepositoryIdentifier::Remote(_)))
                {
                    self.refresh_project_rules_from_metadata(repo_id, ctx);
                }
            }
            RepoMetadataEvent::RepositoryRemoved { id: repo_id } => {
                self.clear_project_rules_for_removed_repository(repo_id, ctx);
            }
            RepoMetadataEvent::UpdatingRepositoryFailed { id } => {
                self.refresh_or_fallback_project_rules_for_repo(id, ctx);
            }
            RepoMetadataEvent::IncrementalUpdateReady { .. } => {}
        }
    }

    fn refresh_or_fallback_project_rules_for_repo(
        &mut self,
        repo_id: &RepositoryIdentifier,
        ctx: &mut ModelContext<Self>,
    ) {
        match repo_id {
            RepositoryIdentifier::Local(_) => {
                let metadata = RepoMetadataModel::as_ref(ctx);
                match metadata.local_repository_coverage(repo_id, ctx) {
                    Some(RepositoryCoverage::Complete) => {
                        self.refresh_project_rules_from_metadata(repo_id, ctx);
                    }
                    Some(RepositoryCoverage::Degraded) => {
                        self.refresh_local_project_rules_from_filesystem(repo_id, ctx);
                    }
                    None if matches!(
                        metadata.repository_state(repo_id, ctx),
                        Some(IndexedRepoState::Failed(_))
                    ) =>
                    {
                        self.refresh_local_project_rules_from_filesystem(repo_id, ctx);
                    }
                    None => {}
                }
            }
            RepositoryIdentifier::Remote(_) => {
                self.refresh_project_rules_from_metadata(repo_id, ctx);
            }
        }
    }

    fn refresh_project_rules_from_metadata(
        &mut self,
        repo_id: &RepositoryIdentifier,
        ctx: &mut ModelContext<Self>,
    ) {
        let refresh_generation = self.advance_refresh_generation(repo_id);
        let Some(root_path) = repo_id.to_local_or_remote_path() else {
            return;
        };
        let rule_paths =
            find_project_rule_files_in_tree(repo_id, RepoMetadataModel::as_ref(ctx), ctx);

        if rule_paths.is_empty() {
            self.apply_project_rules_from_metadata_if_current(
                repo_id,
                refresh_generation,
                root_path,
                Vec::new(),
                ctx,
            );
            return;
        }
        self.spawn_read_project_rules_from_files(
            repo_id.clone(),
            refresh_generation,
            root_path,
            rule_paths,
            ctx,
        );
    }
    fn spawn_read_project_rules_from_files(
        &mut self,
        repo_id: RepositoryIdentifier,
        refresh_generation: u64,
        root_path: LocalOrRemotePath,
        rule_paths: Vec<LocalOrRemotePath>,
        ctx: &mut ModelContext<Self>,
    ) {
        let Some(read_rule_contents) = read_project_rule_contents(rule_paths, ctx) else {
            return;
        };
        ctx.spawn(
            async move {
                let rule_contents = read_rule_contents.await?;
                Ok::<Vec<ProjectRule>, anyhow::Error>(build_project_rules(rule_contents))
            },
            move |me, rules, ctx| match rules {
                Ok(rules) => {
                    me.apply_project_rules_from_metadata_if_current(
                        &repo_id,
                        refresh_generation,
                        root_path,
                        rules,
                        ctx,
                    );
                }
                Err(err) => log::warn!("Failed to read project rules: {err}"),
            },
        );
    }

    fn refresh_local_project_rules_from_filesystem(
        &mut self,
        repo_id: &RepositoryIdentifier,
        ctx: &mut ModelContext<Self>,
    ) {
        self.advance_refresh_generation(repo_id);
        let Some(root_path) = repo_id.local_path_buf() else {
            return;
        };
        ProjectContextModel::handle(ctx).update(ctx, |model, ctx| {
            model.use_local_filesystem_rule_fallback_for_root(&root_path);
            if let Err(error) = model.index_and_store_rules(root_path, ctx) {
                log::warn!("Failed to index project rules from local fallback: {error}");
            }
        });
    }

    fn clear_project_rules_for_removed_repository(
        &mut self,
        repo_id: &RepositoryIdentifier,
        ctx: &mut ModelContext<Self>,
    ) {
        self.refresh_generations.remove(repo_id);
        let Some(root_path) = repo_id.to_local_or_remote_path() else {
            return;
        };
        ProjectContextModel::handle(ctx).update(ctx, |model, ctx| match root_path {
            LocalOrRemotePath::Local(local_root) => {
                model.clear_local_project_rules_for_removed_metadata_root(local_root, ctx);
            }
            LocalOrRemotePath::Remote(remote_root) => {
                model.clear_remote_project_rules_for_removed_metadata_root(remote_root, ctx);
            }
        });
    }

    fn advance_refresh_generation(&mut self, repo_id: &RepositoryIdentifier) -> u64 {
        self.next_refresh_generation += 1;
        self.refresh_generations
            .insert(repo_id.clone(), self.next_refresh_generation);
        self.next_refresh_generation
    }

    fn apply_project_rules_from_metadata_if_current(
        &mut self,
        repo_id: &RepositoryIdentifier,
        refresh_generation: u64,
        root_path: LocalOrRemotePath,
        rules: Vec<ProjectRule>,
        ctx: &mut ModelContext<Self>,
    ) {
        if self.refresh_generations.get(repo_id) != Some(&refresh_generation) {
            return;
        }

        ProjectContextModel::handle(ctx).update(ctx, |model, ctx| match root_path {
            LocalOrRemotePath::Local(local_root) => {
                model.replace_local_project_rules_from_metadata(local_root, rules, ctx);
            }
            LocalOrRemotePath::Remote(remote_root) => {
                model.replace_remote_project_rules_from_metadata(remote_root, rules, ctx);
            }
        });
    }
}

impl Entity for MetadataProjectRulesModel {
    type Event = ();
}

impl SingletonEntity for MetadataProjectRulesModel {}

fn find_project_rule_files_in_tree(
    repo_id: &RepositoryIdentifier,
    repo_metadata: &RepoMetadataModel,
    ctx: &AppContext,
) -> Vec<LocalOrRemotePath> {
    let args = GetContentsArgs::default()
        .include_ignored()
        .with_filter(move |content| {
            let RepoContent::File(file) = content else {
                return false;
            };
            matches_project_rule_file(file.path.file_name())
        });

    repo_metadata
        .get_repo_contents(repo_id, args, ctx)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|content| {
            let RepoContent::File(file) = content else {
                return None;
            };
            match repo_id {
                RepositoryIdentifier::Local(_) => {
                    file.path.to_local_path().map(LocalOrRemotePath::Local)
                }
                RepositoryIdentifier::Remote(remote_root) => Some(LocalOrRemotePath::Remote(
                    RemotePath::new(remote_root.host_id.clone(), file.path.as_ref().clone()),
                )),
            }
        })
        .collect()
}
fn read_project_rule_contents(
    rule_paths: Vec<LocalOrRemotePath>,
    ctx: &AppContext,
) -> Option<ProjectRuleContentsFuture> {
    match rule_paths.first()? {
        LocalOrRemotePath::Local(_) => Some(Box::pin(async move {
            Ok(read_local_project_rule_contents(rule_paths).await)
        })),
        LocalOrRemotePath::Remote(remote) => {
            let client = RemoteServerManager::as_ref(ctx)
                .client_for_host(&remote.host_id)?
                .clone();
            Some(Box::pin(async move {
                let response = client
                    .read_file_context(remote_rule_read_request(&rule_paths))
                    .await?;
                Ok(read_remote_project_rule_contents(
                    rule_paths,
                    response.file_contexts,
                ))
            }))
        }
    }
}

fn remote_rule_read_request(rule_paths: &[LocalOrRemotePath]) -> ReadFileContextRequest {
    ReadFileContextRequest {
        files: rule_paths
            .iter()
            .filter_map(|path| match path {
                LocalOrRemotePath::Remote(remote) => Some(ReadFileContextFile {
                    path: remote.path.as_str().to_string(),
                    line_ranges: Vec::new(),
                }),
                LocalOrRemotePath::Local(_) => None,
            })
            .collect(),
        max_file_bytes: None,
        max_batch_bytes: None,
    }
}

async fn read_local_project_rule_contents(
    rule_paths: Vec<LocalOrRemotePath>,
) -> Vec<(LocalOrRemotePath, String)> {
    let mut rule_contents = Vec::new();
    for path in rule_paths {
        let Some(local_path) = path.to_local_path() else {
            continue;
        };
        match async_fs::read_to_string(local_path).await {
            Ok(content) => rule_contents.push((path, content)),
            Err(error) => log::warn!(
                "Failed to read metadata-backed local project rule {}: {error}",
                local_path.display()
            ),
        }
    }
    rule_contents
}

fn read_remote_project_rule_contents(
    rule_paths: Vec<LocalOrRemotePath>,
    file_contexts: Vec<FileContextProto>,
) -> Vec<(LocalOrRemotePath, String)> {
    let content_by_path = file_contexts
        .into_iter()
        .filter_map(|file_context| {
            let file_context_proto::Content::TextContent(content) = file_context.content? else {
                return None;
            };
            Some((file_context.file_name, content))
        })
        .collect::<HashMap<_, _>>();
    rule_paths
        .into_iter()
        .filter_map(|path| {
            let LocalOrRemotePath::Remote(remote) = &path else {
                return None;
            };
            let content = content_by_path.get(remote.path.as_str())?.clone();
            Some((path, content))
        })
        .collect()
}

fn build_project_rules(rule_contents: Vec<(LocalOrRemotePath, String)>) -> Vec<ProjectRule> {
    rule_contents
        .into_iter()
        .map(|(path, content)| ProjectRule { path, content })
        .collect()
}

fn matches_project_rule_file(file_name: Option<&str>) -> bool {
    file_name.is_some_and(|file_name| {
        file_name.eq_ignore_ascii_case("WARP.md") || file_name.eq_ignore_ascii_case("AGENTS.md")
    })
}

#[cfg(test)]
#[path = "metadata_project_rules_tests.rs"]
mod tests;
