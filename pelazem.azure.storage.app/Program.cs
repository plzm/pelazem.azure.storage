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
		private static string _containerName = "test";
		private static string _queueName = "";
		private static string _accountName = "";
		private static string _key = "";
		private static string _policyName = "";

		static void Main(string[] args)
		{
			Enqueue().Wait();

			//UploadBlob().Wait();

			// ListBlobs().Wait();

			// ProcessBlob(url).Wait();

			Console.WriteLine();
			Console.WriteLine("Done. Press any key to exit.");
			Console.ReadKey();
		}

		static async Task Enqueue()
		{
			ServiceClient svc = new ServiceClient();

			StorageCredentials credentials = svc.GetStorageCredentials(_accountName, _key);
			CloudStorageAccount storageAccount = svc.GetStorageAccount(credentials);

			var result = await svc.Enqueue(storageAccount, _queueName, "This is a test");
		}

		static async Task UploadBlob()
		{
			ServiceClient svc = new ServiceClient();

			StorageCredentials credentials = svc.GetStorageCredentials(_accountName, _key);
			CloudStorageAccount storageAccount = svc.GetStorageAccount(credentials);

			var result = await svc.UploadStringAsync(storageAccount, _containerName, "This is a test", "foo.txt");
		}

		static async Task ListBlobs()
		{
			ServiceClient svc = new ServiceClient();

			StorageCredentials credentials = svc.GetStorageCredentials(_accountName, _key);
			CloudStorageAccount storageAccount = svc.GetStorageAccount(credentials);

			List<ICloudBlob> blobsRaw = (await svc.ListBlobs(storageAccount, _containerName)).ToList();
			List<string> sasUrls = new List<string>();

			foreach (ICloudBlob blob in blobsRaw)
				sasUrls.Add(await svc.GetBlobSAPUrlFromBlobAsync(blob, _policyName));

			Debug.WriteLine(sasUrls.Count);
		}

		static async Task ProcessBlob(string url)
		{
			ServiceClient svc = new ServiceClient();

			StorageCredentials credentials = svc.GetStorageCredentials(_accountName, _key);
			CloudStorageAccount storageAccount = svc.GetStorageAccount(credentials);

			ICloudBlob blob = await svc.GetBlobFromUrlAsync(storageAccount, url);

			string sasUrl = await svc.GetBlobSAPUrlFromBlobAsync(blob, _policyName);

			Debug.WriteLine(sasUrl);
		}
	}
}
