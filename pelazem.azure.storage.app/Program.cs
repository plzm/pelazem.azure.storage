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
		private static string _connString = "";

		static void Main(string[] args)
		{
			// Smoke tests...

			//Enqueue().Wait();

			//UploadBlob().Wait();

			ListBlobs().Wait();

			//ProcessBlob(url).Wait();

			// Reprocess().Wait();

			GetBlobContents().Wait();

			CopyBlob().Wait();

			DeleteBlob().Wait();

			Console.WriteLine();
			Console.WriteLine("Done. Press any key to exit.");
			Console.ReadKey();
		}

		static async Task CopyBlob()
		{
			string sourceContainerName = "source";
			string targetContainerName = "target";

			string sourcePath = "/abc.txt";
			string targetPath = "/foo/abc.txt";

			CloudStorageAccount sa = Common.GetStorageAccount(_connString);

			Blob blob = new Blob();

			OpResult result = await blob.CopyBlockBlobAsync(sa, sourceContainerName, sourcePath, sa, targetContainerName, targetPath);

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
			string targetPath = "/foo/abc.txt";

			CloudStorageAccount sa = Common.GetStorageAccount(_connString);

			Blob blob = new Blob();

			OpResult result = await blob.DeleteBlobByPathAsync(sa, containerName, targetPath);

			Console.WriteLine(result.Succeeded.ToString());
			Console.WriteLine(result.Message);
		}

		static async Task GetBlobContents()
		{
			string blobUrl = "PROVIDE";

			CloudStorageAccount sa = Common.GetStorageAccount(_connString);

			Blob blob = new Blob();

			ICloudBlob b = await blob.GetBlobFromUrlAsync(sa, blobUrl);

			string result = string.Empty;

			using (Stream contents = await blob.GetBlobContentsAsync(b))
			{
				using (StreamReader reader = new StreamReader(contents))
				{
					result = await reader.ReadToEndAsync();
				}
			}

			Console.WriteLine($"{nameof(GetBlobContents)} = {result}");
		}

		//static async Task Reprocess()
		//{
		//	Queue queue = new Queue();

		//	StorageCredentials credentials = Common.GetStorageCredentials(_accountName, _key);

		//	CloudStorageAccount storageAccount = Common.GetStorageAccount(credentials);

		//	OpResult result = await queue.RetryPoisonQueueMessagesAsync(storageAccount, _queueName, 32);
		//}

		static async Task Enqueue()
		{
			Queue queue = new Queue();

			StorageCredentials credentials = Common.GetStorageCredentials(_accountName, _key);

			CloudStorageAccount storageAccount = Common.GetStorageAccount(credentials);

			CloudQueue q  = (await queue.GetQueueAsync(storageAccount, _queueName, true)).Output as CloudQueue;

			var result = await queue.EnqueueMessageAsync(storageAccount, q, "This is a test");

			Console.WriteLine($"{nameof(Enqueue)} = {result.ToString()}");
		}

		static async Task UploadBlob()
		{
			StorageCredentials credentials = Common.GetStorageCredentials(_accountName, _key);

			CloudStorageAccount storageAccount = Common.GetStorageAccount(credentials);

			Blob blob = new Blob();

			var result = await blob.UploadStringAsync(storageAccount, _containerName, "This is a test", "foo.txt");

			Console.WriteLine($"{nameof(UploadBlob)} = {result.ToString()}");
		}

		static async Task ListBlobs()
		{
			CloudStorageAccount storageAccount = Common.GetStorageAccount(_connString);

			Blob blob = new Blob();

			List<ICloudBlob> blobs = (await blob.ListBlobsAsync(storageAccount, "source", "inbound/")).ToList();

			List<string> sasUrls = new List<string>();

			foreach (ICloudBlob cloudBlob in blobs)
				sasUrls.Add(await blob.GetBlobSAPUrlFromBlobAsync(cloudBlob, _policyName));

			Debug.WriteLine(sasUrls.Count);
		}

		//static async Task ProcessBlob(string url)
		//{
		//	StorageCredentials credentials = Common.GetStorageCredentials(_accountName, _key);

		//	CloudStorageAccount storageAccount = Common.GetStorageAccount(credentials);

		//	Blob blob = new Blob();

		//	ICloudBlob cloudBlob = await blob.GetBlobFromUrlAsync(storageAccount, url);

		//	string sasUrl = await blob.GetBlobSAPUrlFromBlobAsync(cloudBlob, _policyName);

		//	Debug.WriteLine(sasUrl);
		//}
	}
}
