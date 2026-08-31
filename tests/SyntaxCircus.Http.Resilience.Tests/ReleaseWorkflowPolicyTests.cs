using System.Text.RegularExpressions;

namespace SyntaxCircus.Http.Resilience.Tests;

public sealed class ReleaseWorkflowPolicyTests
{
    private static readonly string RepositoryRoot = FindRepositoryRoot();
    private static readonly string Workflow = File.ReadAllText(Path.Combine(RepositoryRoot, ".github", "workflows", "build.yml"));
    private static readonly string PublishScript = File.ReadAllText(Path.Combine(RepositoryRoot, "publish.ps1"));

    [Fact]
    public void Ordinary_pushes_and_pull_requests_cannot_pack_or_publish()
    {
        Workflow.ShouldContain("workflow_dispatch:");
        Workflow.ShouldContain("if: github.event_name == 'workflow_dispatch'");
        Workflow.ShouldNotContain("if: github.ref == 'refs/heads/main' && github.event_name == 'push'");
    }

    [Fact]
    public void Manual_candidate_requires_an_exact_version_and_main_source_sha()
    {
        Workflow.ShouldContain("version:");
        Workflow.ShouldContain("source_sha:");
        Workflow.ShouldContain("0.2.0-cmsify.1");
        Workflow.ShouldContain("github.ref == 'refs/heads/main'");
        Workflow.ShouldContain("format('http-resilience-release-{0}', inputs.version)");
        Workflow.ShouldContain("cancel-in-progress: false");
        Workflow.ShouldContain("git merge-base --is-ancestor \"$SOURCE_SHA\" origin/main");
        Workflow.ShouldContain("test \"$(git rev-parse HEAD)\" = \"$SOURCE_SHA\"");
    }

    [Fact]
    public void Protected_publication_reuses_and_verifies_the_uploaded_candidate()
    {
        Workflow.ShouldContain("environment: release");
        Workflow.ShouldContain("actions/upload-artifact@");
        Workflow.ShouldContain("actions/download-artifact@");
        Workflow.ShouldContain("sha256sum --check SHA256SUMS");
        Workflow.ShouldContain("Reject an existing NuGet version");
        Workflow.ShouldNotContain("--skip-duplicate");
    }

    [Fact]
    public void Candidate_build_generates_documentation_required_by_no_build_pack()
    {
        var buildStart = Workflow.IndexOf("- name: Restore, build, and test candidate source", StringComparison.Ordinal);
        var packStart = Workflow.IndexOf("- name: Pack exact candidate once", StringComparison.Ordinal);

        buildStart.ShouldBeGreaterThanOrEqualTo(0);
        packStart.ShouldBeGreaterThan(buildStart);
        Workflow[buildStart..packStart].ShouldContain("-p:GenerateDocumentationFile=true");
        Workflow[buildStart..packStart].ShouldContain("'-p:NoWarn=CS1591;CS1573'");
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
        PublishScript.ShouldContain("workflow_dispatch");
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
