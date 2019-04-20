using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using pelazem.util;

namespace pelazem.azure.storage
{
	class Program
	{
		private static string _accountName = "";
		private static string _key = "";
		private static string _containerName = "";
		private static string _queueName = "";
		private static string _policyName = "";

		static void Main(string[] args)
		{
			// Enqueue().Wait();

			//UploadBlob().Wait();

			// ListBlobs().Wait();

			// ProcessBlob(url).Wait();

			Reprocess().Wait();

			Console.WriteLine();
			Console.WriteLine("Done. Press any key to exit.");
			Console.ReadKey();
		}

		static async Task Reprocess()
		{
			ServiceClient storageClient = new ServiceClient();
			StorageCredentials credentials = storageClient.GetStorageCredentials(_accountName, _key);
			CloudStorageAccount storageAccount = storageClient.GetStorageAccount(credentials);

			OpResult result = await storageClient.RetryPoisonQueueMessagesAsync(storageAccount, _queueName, 32);
		}

		static async Task Enqueue()
		{
			ServiceClient storageClient = new ServiceClient();

			StorageCredentials credentials = storageClient.GetStorageCredentials(_accountName, _key);
			CloudStorageAccount storageAccount = storageClient.GetStorageAccount(credentials);
			CloudQueue queue = (await storageClient.GetQueueAsync(storageAccount, _queueName, true)).Output;

			var result = await storageClient.EnqueueMessageAsync(storageAccount, queue, "This is a test");
		}

		static async Task UploadBlob()
		{
			ServiceClient svc = new ServiceClient();

			StorageCredentials credentials = svc.GetStorageCredentials(_accountName, _key);
			CloudStorageAccount storageAccount = svc.GetStorageAccount(credentials);

			var result = await svc.UploadStringAsync(storageAccount, _containerName, "This is a test", "foo.txt");
		}

		static async Task<IList<ICloudBlob>> ListBlobs()
		{
			ServiceClient svc = new ServiceClient();

			StorageCredentials credentials = svc.GetStorageCredentials(_accountName, _key);
			CloudStorageAccount storageAccount = svc.GetStorageAccount(credentials);

			List<ICloudBlob> blobs = (await svc.ListBlobs(storageAccount, _containerName, "root/sub/folder/", 1000)).ToList();

			//List<string> sasUrls = new List<string>();

			//foreach (ICloudBlob blob in blobs)
			//	sasUrls.Add(await svc.GetBlobSAPUrlFromBlobAsync(blob, _policyName));

			//Debug.WriteLine(sasUrls.Count);

			return blobs;
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
