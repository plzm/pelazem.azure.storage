using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace pelazem.azure.storage
{
	class Program
	{
		static void Main(string[] args)
		{
			ProcessBlob().Wait();

			Console.WriteLine();
			Console.WriteLine("Done. Press any key to exit.");
			Console.ReadKey();
		}

		static async Task ProcessBlob()
		{
			string containerName = "";
			string accountName = "";
			string key = "";

			StorageConfig config = new StorageConfig() { BlobContainerName = containerName, StorageAccountName = accountName, StorageAccountKey = key };

			ServiceClient svc = new ServiceClient();

			ICloudBlob blob = await svc.GetBlobFromUrlAsync(config, "");

			string url = await svc.GetBlobSharedAccessUrlAsync(blob, "");

			Debug.WriteLine(url);
		}
	}
}
