using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
			string containerName = "";
			string accountName = "";
			string key = "";
			string policyName = "";

			StorageConfig config = new StorageConfig() { ContainerName = containerName, StorageAccountName = accountName, StorageAccountKey = key };

			// ProcessBlob(config, url, policyName).Wait();

			ListBlobs(config, policyName).Wait();

			Console.WriteLine();
			Console.WriteLine("Done. Press any key to exit.");
			Console.ReadKey();
		}

		static async Task ListBlobs(StorageConfig config, string policyName)
		{
			ServiceClient svc = new ServiceClient();

			List<ICloudBlob> blobsRaw = (await svc.ListBlobs(config)).ToList();
			List<string> sasUrls = new List<string>();

			foreach (ICloudBlob blob in blobsRaw)
				sasUrls.Add(await svc.GetBlobSAPUrlFromBlobAsync(blob, policyName));

			Debug.WriteLine(sasUrls.Count);
		}

		static async Task ProcessBlob(StorageConfig config, string url, string policyName)
		{
			ServiceClient svc = new ServiceClient();

			ICloudBlob blob = await svc.GetBlobFromUrlAsync(config, url);

			string sasUrl = await svc.GetBlobSAPUrlFromBlobAsync(blob, policyName);

			Debug.WriteLine(sasUrl);
		}
	}
}
