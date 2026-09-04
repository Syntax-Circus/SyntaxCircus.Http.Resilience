using System.Text.RegularExpressions;

namespace SyntaxCircus.Http.Resilience.Tests;

public sealed class ReleaseWorkflowPolicyTests
{
    private static readonly string RepositoryRoot = FindRepositoryRoot();
    private static readonly string Workflow = File.ReadAllText(Path.Combine(RepositoryRoot, ".github", "workflows", "build.yml"));
    private static readonly string PublishScript = File.ReadAllText(Path.Combine(RepositoryRoot, "publish.ps1"));

    [Fact]
    public void Only_main_pushes_can_pack_or_publish()
    {
        Workflow.ShouldContain("if: github.ref == 'refs/heads/main' && github.event_name == 'push'");
        Workflow.ShouldNotContain("workflow_dispatch:");
        Workflow.ShouldNotContain("github.event_name == 'workflow_dispatch'");
    }

    [Fact]
    public void Main_push_build_packs_and_uploads_the_versioned_package()
    {
        Workflow.ShouldContain("- name: Pack");
        Workflow.ShouldContain("dotnet pack SyntaxCircus.Http.Resilience.slnx --no-build --configuration Release --output artifacts");
        Workflow.ShouldContain("- name: Upload package artifact");
        Workflow.ShouldContain("name: nuget-package");
        Workflow.ShouldContain("cancel-in-progress: false");
    }

    [Fact]
    public void Protected_publication_uses_the_uploaded_package_and_trusted_publishing()
    {
        Workflow.ShouldContain("environment: release");
        Workflow.ShouldContain("actions/upload-artifact@");
        Workflow.ShouldContain("actions/download-artifact@");
        Workflow.ShouldContain("NuGet/login@");
        Workflow.ShouldContain("dotnet nuget push artifacts/*.nupkg");
        Workflow.ShouldContain("--skip-duplicate");
    }

    [Fact]
    public void Publication_tag_is_derived_from_the_generated_package()
    {
        Workflow.ShouldContain("find artifacts -maxdepth 1 -type f -name '*.nupkg' ! -name '*.snupkg'");
        Workflow.ShouldContain("version=${package_path##*/SyntaxCircus.Http.Resilience.}");
        Workflow.ShouldContain("tag=\"v$version\"");
        Workflow.ShouldContain("git tag \"$tag\" \"$GITHUB_SHA\"");
    }

    [Fact]
    public void Actions_are_immutable_and_local_script_cannot_publish()
    {
        var actionReferences = Regex.Matches(Workflow, @"uses:\s*[^\s]+@(?<reference>[^\s#]+)");
        actionReferences.Count.ShouldBeGreaterThan(0);
        foreach (Match actionReference in actionReferences)
        {
            actionReference.Groups["reference"].Value.ShouldMatch("^[0-9a-f]{40}$");
        }

        PublishScript.ShouldNotContain("dotnet nuget push");
        PublishScript.ShouldContain("main-push release workflow");
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "SyntaxCircus.Http.Resilience.slnx")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("Repository root was not found.");
    }
}
