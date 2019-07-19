using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace pelazem.azure.storage
{
	public class BlobInfo
	{
		private BlobInfo() { }

		public BlobInfo(string blobUri)
		{
			Uri uri = new Uri(blobUri);

			this.Scheme = uri.Scheme;
			this.Host = uri.Host;
			this.AbsolutePath = uri.AbsolutePath;

			List<string> uriSegments = uri.Segments.ToList();

			this.ContainerName = uriSegments[1].Substring(0, uriSegments[1].Length - 1);
			this.FileName = uriSegments.Last();

			// Prepare folder path
			uriSegments.RemoveAt(uriSegments.Count - 1);
			uriSegments.RemoveAt(1);
			uriSegments.RemoveAt(0);

			string folderPath = uriSegments.Aggregate("", (output, next) => output + next).Trim();
			this.FolderPath = (string.IsNullOrWhiteSpace(folderPath) ? "/" : folderPath);
		}

		public string Scheme { get; set; }
		public string Host { get; set; }
		public string AbsolutePath { get; set; }
		public string ContainerName { get; set; }
		public string FolderPath { get; set; }
		public string FileName { get; set; }
		public string FullFolderFilePath { get { return (this.FolderPath != "/" ? this.FolderPath : string.Empty) + this.FileName; } }
	}
}
