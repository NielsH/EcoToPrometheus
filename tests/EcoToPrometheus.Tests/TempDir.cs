namespace EcoToPrometheus.Tests
{
    using System;
    using System.IO;

    /// <summary>A fresh directory under the system temp path, deleted on dispose. One per test.</summary>
    sealed class TempDir : IDisposable
    {
        public string Path { get; }

        public TempDir()
        {
            this.Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "EcoToPrometheus.Tests-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(this.Path);
        }

        public string File(string name) => System.IO.Path.Combine(this.Path, name);

        public void Dispose()
        {
            try { Directory.Delete(this.Path, recursive: true); }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
    }
}
