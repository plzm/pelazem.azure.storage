using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Queue;
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
		private static string _connString = "DefaultEndpointsProtocol=https;AccountName=fbtest9ecommsa;AccountKey=RDVwGJPLeJWZcEPZNubwfFxZrnNjhW3Qj1zoAxJ2HVxb8eWQWDo7gHgRUEeP2DjKjT2pBNwLMQkBJKlZ7gVKqw==;EndpointSuffix=core.windows.net";

		static void Main(string[] args)
		{
			// Smoke tests...

			// Enqueue().Wait();

			// UploadBlob().Wait();

			ListBlobs().Wait();

			// ProcessBlob(url).Wait();

			// Reprocess().Wait();

			// GetBlobContents().Wait();

			// CopyBlob().Wait();

			// DeleteBlob().Wait();

			Console.WriteLine();
			Console.WriteLine("Done. Press any key to exit.");
			Console.ReadKey();
		}

		static async Task CopyBlob()
		{
			string sourceContainerName = "source";
			string targetContainerName = "target";

			string sourcePath = "/upload-shipment-body.txt";
			string targetPath = "/foo/upload-shipment-body-bar.txt";

			ServiceClient storageClient = new ServiceClient();
			CloudStorageAccount sa = storageClient.GetStorageAccount(_connString);

			OpResult result = await storageClient.CopyBlockBlobAsync(sa, sourceContainerName, sourcePath, sa, targetContainerName, targetPath);

			CloudBlockBlob targetBlob = result.Output as CloudBlockBlob;

			while (targetBlob.CopyState.Status == CopyStatus.Pending)
			{
				Console.WriteLine("Waiting...");
				Thread.Sleep(500);
			}

			Console.WriteLine($"Done with status {targetBlob.CopyState.Status.ToString()}");
		}

		static async Task DeleteBlob()
		{
			string containerName = "target";
			string targetPath = "/foo/upload-shipment-body-bar.txt";

			ServiceClient storageClient = new ServiceClient();
			CloudStorageAccount sa = storageClient.GetStorageAccount(_connString);

			OpResult result = await storageClient.DeleteBlobByPathAsync(sa, containerName, targetPath);

			Console.WriteLine(result.Succeeded.ToString());
			Console.WriteLine(result.Message);
		}

		static async Task GetBlobContents()
		{
			string blobUrl = "";

			ServiceClient storageClient = new ServiceClient();
			CloudStorageAccount sa = storageClient.GetStorageAccount(_connString);
			ICloudBlob b = await storageClient.GetBlobFromUrlAsync(sa, blobUrl);

			string result = string.Empty;

			using (Stream contents = await storageClient.GetBlobContentsAsync(b))
			{
				using (StreamReader reader = new StreamReader(contents))
				{
					result = await reader.ReadToEndAsync();
				}
			}

			Console.WriteLine(result);
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

		static async Task ListBlobs()
		{
			ServiceClient svc = new ServiceClient();
			CloudStorageAccount storageAccount = svc.GetStorageAccount(_connString);

			List<ICloudBlob> blobs = (await svc.ListBlobs(storageAccount, "source", "inbound/")).ToList();

			List<string> sasUrls = new List<string>();

			foreach (ICloudBlob blob in blobs)
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
