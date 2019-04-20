using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using pelazem.util;

// Reference https://docs.microsoft.com/en-us/azure/storage/blobs/storage-dotnet-shared-access-signature-part-2

namespace pelazem.azure.storage
{
	public class ServiceClient
	{
		#region Get Blob URL

		public async Task<string> GetBlobUrlFromBlobPathAsync(CloudStorageAccount storageAccount, string containerName, string blobPath)
		{
			string result = string.Empty;

			if (storageAccount == null || string.IsNullOrWhiteSpace(containerName) || string.IsNullOrWhiteSpace(blobPath))
				return result;

			CloudBlob blob = await this.GetBlobFromPathAsync(storageAccount, containerName, blobPath);

			result = blob.Uri.AbsoluteUri;

			return result;
		}

		public async Task<string> GetBlobSAPUrlFromBlobPathAsync(CloudStorageAccount storageAccount, string containerName, string blobPath, string sharedAccessPolicyName)
		{
			string result = string.Empty;

			if (storageAccount == null || string.IsNullOrWhiteSpace(containerName) || string.IsNullOrWhiteSpace(blobPath) || string.IsNullOrWhiteSpace(sharedAccessPolicyName))
				return result;

			CloudBlob blob = await this.GetBlobFromPathAsync(storageAccount, containerName, blobPath);

			CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

			SharedAccessBlobPolicy policy = await this.GetSharedAccessPolicy(container, sharedAccessPolicyName);

			if (policy != null)
				result = blob.Uri.AbsoluteUri + blob.GetSharedAccessSignature(policy);

			return result;
		}

		public async Task<string> GetBlobSAPUrlFromBlobAsync(ICloudBlob blob, string sharedAccessPolicyName)
		{
			string result = string.Empty;

			if (blob == null || string.IsNullOrWhiteSpace(sharedAccessPolicyName))
				return result;

			SharedAccessBlobPolicy policy = await this.GetSharedAccessPolicy(blob.Container, sharedAccessPolicyName);

			if (policy != null)
				result = blob.Uri.AbsoluteUri + blob.GetSharedAccessSignature(policy);

			return result;
		}

		public async Task<string> GetBlobSAPUrlFromBlobUrlAsync(CloudStorageAccount storageAccount, string blobUrl, string sharedAccessPolicyName)
		{
			string result = string.Empty;

			if (storageAccount == null || string.IsNullOrWhiteSpace(blobUrl) || string.IsNullOrWhiteSpace(sharedAccessPolicyName))
				return result;

			ICloudBlob blob = await GetBlobFromUrlAsync(storageAccount, blobUrl);

			result = await GetBlobSAPUrlFromBlobAsync(blob, sharedAccessPolicyName);

			return result;
		}

		public async Task<string> GetBlobSASUrlFromBlobPathAsync(CloudStorageAccount storageAccount, string containerName, string blobPath, DateTimeOffset expiryDateTime, SharedAccessBlobPermissions policyPermissions = SharedAccessBlobPermissions.Read)
		{
			string result = string.Empty;

			if (storageAccount == null || string.IsNullOrWhiteSpace(containerName) || string.IsNullOrWhiteSpace(blobPath))
				return result;

			CloudBlob blob = await this.GetBlobFromPathAsync(storageAccount, containerName, blobPath);

			SharedAccessBlobPolicy sasBlobPolicy = new SharedAccessBlobPolicy();
			sasBlobPolicy.SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5); // For clock skew
			sasBlobPolicy.SharedAccessExpiryTime = expiryDateTime;
			sasBlobPolicy.Permissions = policyPermissions;

			//Generate the shared access signature on the blob, setting the constraints directly on the signature.
			string sasBlobToken = blob.GetSharedAccessSignature(sasBlobPolicy);

			result = blob.Uri.AbsoluteUri + sasBlobToken;

			return result;
		}

		public string GetBlobSASUrlFromBlob(ICloudBlob blob, DateTimeOffset expiryDateTime, SharedAccessBlobPermissions policyPermissions = SharedAccessBlobPermissions.Read)
		{
			string result = string.Empty;

			if (blob == null)
				return result;

			SharedAccessBlobPolicy sasBlobPolicy = new SharedAccessBlobPolicy();
			sasBlobPolicy.SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5); // For clock skew
			sasBlobPolicy.SharedAccessExpiryTime = expiryDateTime;
			sasBlobPolicy.Permissions = policyPermissions;

			//Generate the shared access signature on the blob, setting the constraints directly on the signature.
			string sasBlobToken = blob.GetSharedAccessSignature(sasBlobPolicy);

			result = blob.Uri.AbsoluteUri + sasBlobToken;

			return result;
		}

		#endregion

		#region Get Blob

		public CloudBlob GetBlobFromPath(CloudBlobContainer container, string targetBlobPath)
		{
			if (container == null || string.IsNullOrWhiteSpace(targetBlobPath))
				return null;

			CloudBlob blob = container.GetBlobReference(targetBlobPath);

			return blob;
		}

		public async Task<CloudBlob> GetBlobFromPathAsync(CloudStorageAccount storageAccount, string containerName, string targetBlobPath)
		{
			if (storageAccount == null || string.IsNullOrWhiteSpace(containerName) || string.IsNullOrWhiteSpace(targetBlobPath))
				return null;

			CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

			CloudBlob blob = this.GetBlobFromPath(container, targetBlobPath);

			return blob;
		}

		public async Task<ICloudBlob> GetBlobFromUrlAsync(CloudStorageAccount storageAccount, string blobUrl)
		{
			if (storageAccount == null || string.IsNullOrWhiteSpace(blobUrl))
				return null;

			CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

			ICloudBlob result = await blobClient.GetBlobReferenceFromServerAsync(new Uri(blobUrl));

			return result;
		}

		#endregion

		#region List Blobs

		public async Task<IEnumerable<ICloudBlob>> ListBlobs(CloudStorageAccount storageAccount, string containerName, string blobNamePrefix = "", int? maxResultsPerSegment = 50)
		{
			CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

			List<ICloudBlob> result = new List<ICloudBlob>();

			BlobContinuationToken token = null;

			do
			{
				var response = await container.ListBlobsSegmentedAsync(blobNamePrefix, true, BlobListingDetails.Metadata, maxResultsPerSegment, token, null, null);

				token = response.ContinuationToken;

				result.AddRange(response.Results.Where(b => b is ICloudBlob).Select(b => b as ICloudBlob));
			}
			while (token != null);

			return result;
		}

		#endregion

		#region Upload Blob

		public async Task<OpResult> UploadFileFromUrlAsync(CloudStorageAccount storageAccount, string containerName, string sourceFileUrl, string targetBlobPath)
		{
			OpResult result = new OpResult() { Succeeded = false };

			if (storageAccount == null)
			{
				result.Message = $"Parameter {nameof(storageAccount)} was null.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(containerName))
			{
				result.Message = $"Parameter {nameof(containerName)} was null or zero-length.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(sourceFileUrl))
			{
				result.Message = $"Parameter {nameof(sourceFileUrl)} was empty or whitespace.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(targetBlobPath))
			{
				result.Message = $"Parameter {nameof(targetBlobPath)} was empty or whitespace.";
				return result;
			}

			try
			{
				WebRequest request = WebRequest.Create(sourceFileUrl);

				request.Timeout = 120000;
				request.UseDefaultCredentials = true;
				request.Proxy.Credentials = request.Credentials;

				using (WebResponse response = await request.GetResponseAsync())
				{
					using (Stream stream = response.GetResponseStream())
					{
						result = await this.UploadStreamAsync(storageAccount, containerName, stream, targetBlobPath);
					}
				}
			}
			catch (Exception ex)
			{
				// TODO log exception

				result.Succeeded = false;
				result.Message = "Error! Exception caught. See Output property for Exception.";
				result.Output = ex;
			}

			return result;
		}

		public async Task<OpResult> UploadFileFromLocalAsync(CloudStorageAccount storageAccount, string containerName, string sourceFilePath, string targetBlobPath)
		{
			OpResult result = new OpResult() { Succeeded = false };

			if (storageAccount == null)
			{
				result.Message = $"Parameter {nameof(storageAccount)} was null.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(containerName))
			{
				result.Message = $"Parameter {nameof(containerName)} was null or zero-length.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(sourceFilePath))
			{
				result.Message = $"Parameter {nameof(sourceFilePath)} was empty or whitespace.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(targetBlobPath))
			{
				result.Message = $"Parameter {nameof(targetBlobPath)} was empty or whitespace.";
				return result;
			}

			if (!File.Exists(sourceFilePath))
			{
				result.Message = $"Parameter {nameof(sourceFilePath)} file does not exist.";
				return result;
			}

			try
			{
				using (FileStream stream = File.OpenRead(sourceFilePath))
				{
					result = await this.UploadStreamAsync(storageAccount, containerName, stream, targetBlobPath);
				}
			}
			catch (Exception ex)
			{
				// TODO log exception

				result.Succeeded = false;
				result.Message = "Error! Exception caught. See Output property for Exception.";
				result.Output = ex;
			}

			return result;
		}

		public async Task<OpResult> UploadStringAsync(CloudStorageAccount storageAccount, string containerName, string contents, string targetBlobPath)
		{
			byte[] bytes = Encoding.UTF8.GetBytes(contents);

			return await UploadByteArrayAsync(storageAccount, containerName, bytes, targetBlobPath);
		}

		public async Task<OpResult> UploadStreamAsync(CloudStorageAccount storageAccount, string containerName, Stream sourceStream, string targetBlobPath)
		{
			OpResult result = new OpResult() { Succeeded = false };

			if (storageAccount == null)
			{
				result.Message = $"Parameter {nameof(storageAccount)} was null.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(containerName))
			{
				result.Message = $"Parameter {nameof(containerName)} was null or zero-length.";
				return result;
			}

			if (sourceStream == null || sourceStream.Length == 0)
			{
				result.Message = $"Parameter {nameof(sourceStream)} was null or zero-length.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(targetBlobPath))
			{
				result.Message = $"Parameter {nameof(targetBlobPath)} was empty or whitespace.";
				return result;
			}

			try
			{
				byte[] bytes;

				using (var memoryStream = new MemoryStream())
				{
					await sourceStream.CopyToAsync(memoryStream);

					bytes = memoryStream.ToArray();
				}

				result = await this.UploadByteArrayAsync(storageAccount, containerName, bytes, targetBlobPath);
			}
			catch (Exception ex)
			{
				// TODO log exception

				result.Succeeded = false;
				result.Message = "Error! Exception caught. See Output property for Exception.";
				result.Output = ex;
			}

			return result;
		}

		public async Task<OpResult> UploadByteArrayAsync(CloudStorageAccount storageAccount, string containerName, byte[] bytes, string targetBlobPath)
		{
			OpResult result = new OpResult() { Succeeded = false };

			if (storageAccount == null)
			{
				result.Message = $"Parameter {nameof(storageAccount)} was null.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(containerName))
			{
				result.Message = $"Parameter {nameof(containerName)} was null or zero-length.";
				return result;
			}


			if (bytes == null || bytes.Length == 0)
			{
				result.Message = $"Parameter {nameof(bytes)} was null or zero-length.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(targetBlobPath))
			{
				result.Message = $"Parameter {nameof(targetBlobPath)} was empty or whitespace.";
				return result;
			}

			try
			{
				CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

				CloudBlockBlob blob = container.GetBlockBlobReference(targetBlobPath);

				// Upload the file
				await blob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);

				// Result is success if the blob exists - the upload operation does not return a status so we check success after the upload
				result.Succeeded = await blob.ExistsAsync();
			}
			catch (Exception ex)
			{
				// TODO log exception

				result.Succeeded = false;
				result.Message = "Error! Exception caught. See Output property for Exception.";
				result.Output = ex;
			}

			return result;
		}

		#endregion

		#region Shared Access Policy

		public async Task<SharedAccessBlobPolicy> GetSharedAccessPolicy(CloudBlobContainer container, string policyName)
		{
			SharedAccessBlobPolicy policy = null;

			BlobContainerPermissions permissions = await container.GetPermissionsAsync();

			if (permissions.SharedAccessPolicies.ContainsKey(policyName))
				policy = permissions.SharedAccessPolicies.FirstOrDefault(p => p.Key == policyName).Value;

			return policy;
		}

		public async Task CreateSharedAccessPolicy(CloudBlobClient blobClient, CloudBlobContainer container, string policyName, DateTimeOffset? expiryDateTime, SharedAccessBlobPermissions policyPermissions = SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read)
		{
			//Get the container's existing permissions.
			BlobContainerPermissions permissions = await container.GetPermissionsAsync();

			//Create a new shared access policy and define its constraints.
			SharedAccessBlobPolicy sharedPolicy = new SharedAccessBlobPolicy()
			{
				SharedAccessExpiryTime = expiryDateTime,
				Permissions = policyPermissions
			};

			//Add the new policy to the container's permissions, and set the container's permissions.
			permissions.SharedAccessPolicies.Add(policyName, sharedPolicy);

			await container.SetPermissionsAsync(permissions);
		}

		#endregion

		#region Blob

		public CloudBlobClient GetBlobClient(CloudStorageAccount storageAccount)
		{
			return storageAccount.CreateCloudBlobClient();
		}

		public async Task<CloudBlobContainer> GetContainerAsync(CloudStorageAccount storageAccount, string containerName, bool createIfNotExists, BlobContainerPublicAccessType publicAccessIfCreating = BlobContainerPublicAccessType.Off)
		{
			CloudBlobContainer result = null;

			if (storageAccount == null || string.IsNullOrWhiteSpace(containerName))
				return result;

			CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

			try
			{
				result = blobClient.GetContainerReference(containerName);

				if (!(await result.ExistsAsync()) && createIfNotExists)
				{
					bool created = await result.CreateIfNotExistsAsync();

					if (created)
						await result.SetPermissionsAsync(new BlobContainerPermissions() { PublicAccess = publicAccessIfCreating });
				}
			}
			catch (Exception ex)
			{
				// TODO log exception

				result = null;
			}

			return result;
		}

		#endregion

		#region Queue

		public async Task<OpResult<CloudQueue>> GetQueueAsync(CloudStorageAccount storageAccount, string queueName, bool createIfNotExists = true)
		{
			OpResult<CloudQueue> result = new OpResult<CloudQueue>();

			result.ValidationResult.Validations.AddItems(new[] { ValidateStorageAccount(storageAccount), ValidateQueueName(queueName) });

			if (!(result.ValidationResult.IsValid))
				return result;

			try
			{
				CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

				CloudQueue queue = queueClient.GetQueueReference(queueName);

				if (createIfNotExists)
					await queue.CreateIfNotExistsAsync();

				result.Output = queue;
				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Exception = ex;
				result.Output = null;
				result.Succeeded = false;
			}

			return result;
		}

		/// <summary>
		/// Wraps https://docs.microsoft.com/en-us/dotnet/api/microsoft.windowsazure.storage.queue.cloudqueue.getmessagesasync.
		/// 
		/// NOTE: this retrieves messages but DOES NOT DELETE them from the queue. The retrieved messages are marked invisible for the default invisibility timeout.
		/// You - YES YOU - MUST explicitly delete messages off the queue so they do not "reappear".
		/// </summary>
		/// <param name="storageAccount"></param>
		/// <param name="queueName"></param>
		/// <param name="messageCount">Between 1 and 32. If outside this range, will be corrected internally to 32.</param>
		/// <returns></returns>
		public async Task<OpResult<List<CloudQueueMessage>>> GetQueueMessagesAsync(CloudStorageAccount storageAccount, CloudQueue queue, int messageCount = 32)
		{
			if (messageCount <= 0 || messageCount > 32)
				messageCount = 32;

			OpResult<List<CloudQueueMessage>> result = new OpResult<List<CloudQueueMessage>>();

			result.ValidationResult.Validations.AddItems(new[] { ValidateStorageAccount(storageAccount), ValidateQueue(queue) });

			if (!(result.ValidationResult.IsValid))
				return result;

			try
			{
				result.Output = (await queue.GetMessagesAsync(messageCount)).ToList();
				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Succeeded = false;
				result.Exception = ex;
			}

			return result;
		}

		public async Task<OpResult> EnqueueMessageAsync(CloudStorageAccount storageAccount, CloudQueue queue, string queueMessage, TimeSpan? timeToLiveOnQueue = null, TimeSpan? timeBeforeVisible = null)
		{
			OpResult result = new OpResult();

			result.ValidationResult.Validations.AddItems(new[] { ValidateQueueMessage(queueMessage) });

			if (!result.ValidationResult.IsValid)
				return result;

			CloudQueueMessage msg = new CloudQueueMessage(queueMessage);

			return await EnqueueMessageAsync(storageAccount, queue, msg, timeToLiveOnQueue, timeBeforeVisible);
		}

		public async Task<OpResult> EnqueueMessageAsync(CloudStorageAccount storageAccount, CloudQueue queue, CloudQueueMessage queueMessage, TimeSpan? timeToLiveOnQueue = null, TimeSpan? timeBeforeVisible = null)
		{
			OpResult result = new OpResult();

			result.ValidationResult.Validations.AddItems(new[] { ValidateStorageAccount(storageAccount), ValidateQueue(queue), ValidateQueueMessage(queueMessage) });

			if (!result.ValidationResult.IsValid)
				return result;

			try
			{
				await queue.AddMessageAsync(queueMessage, timeToLiveOnQueue, timeBeforeVisible, null, null);

				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Succeeded = false;
				result.Exception = ex;
			}

			return result;
		}

		public async Task<OpResult> DeleteMessageAsync(CloudStorageAccount storageAccount, CloudQueue queue, CloudQueueMessage queueMessage)
		{
			OpResult result = new OpResult() { Succeeded = false } ;

			result.ValidationResult.Validations.AddItems(new[] { ValidateStorageAccount(storageAccount), ValidateQueue(queue), ValidateQueueMessage(queueMessage) });

			if (!result.ValidationResult.IsValid)
				return result;

			try
			{
				await queue.DeleteMessageAsync(queueMessage);

				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Succeeded = false;
				result.Exception = ex;
			}

			return result;
		}

		public async Task<OpResult> DeleteMessagesAsync(CloudStorageAccount storageAccount, CloudQueue queue, IEnumerable<CloudQueueMessage> queueMessages)
		{
			OpResult result = new OpResult() { Succeeded = false };

			result.ValidationResult.Validations.AddItems(new[] { ValidateStorageAccount(storageAccount), ValidateQueue(queue), ValidateQueueMessages(queueMessages) });

			if (!result.ValidationResult.IsValid)
				return result;

			try
			{
				foreach(CloudQueueMessage queueMessage in queueMessages.Where(qm => qm != null))
					await queue.DeleteMessageAsync(queueMessage);

				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Succeeded = false;
				result.Exception = ex;
			}

			return result;
		}

		/// <summary>
		/// Removes all messages from the poison queue and tries them again. Processes batches of size batchSize.
		/// </summary>
		/// <param name="storageAccount"></param>
		/// <param name="queueName">The real queue - NOT the poison queue</param>
		/// <param name="batchSize">Number of messages to process at a time. Between 1 and 32.</param>
		/// <returns></returns>
		public async Task<OpResult> RetryPoisonQueueMessagesAsync(CloudStorageAccount storageAccount, string queueName, int batchSize)
		{
			OpResult result = new OpResult();

			OpResult<CloudQueue> queueResult = await GetQueueAsync(storageAccount, queueName, false);

			result.ValidationResult.Validations.AddItems(queueResult.ValidationResult.Validations);

			if (!result.ValidationResult.IsValid)
				return result;

			CloudQueue queue = null;

			if (queueResult.Succeeded && queueResult.Output != null)
				queue = queueResult.Output;
			else
			{
				result.Message = "Queue not found.";
				return result;
			}

			string poisonQueueName = queueName + "-poison";
			CloudQueue poisonQueue = null;

			OpResult<CloudQueue> poisonQueueResult = await GetQueueAsync(storageAccount, poisonQueueName, false);

			if (poisonQueueResult.Succeeded && poisonQueueResult.Output != null)
				poisonQueue = poisonQueueResult.Output;
			else
			{
				result.Message = "Poison Queue not found.";
				return result;
			}

			// We now have the queue and the poison queue
			List<CloudQueueMessage> messagesToDeleteFromPoisonQueue = new List<CloudQueueMessage>();

			while (true)
			{
				messagesToDeleteFromPoisonQueue.Clear();

				List<CloudQueueMessage> poisonQueueMessages = null;

				OpResult<List<CloudQueueMessage>> poisonQueueMessagesResult = await GetQueueMessagesAsync(storageAccount, poisonQueue, batchSize);

				if (poisonQueueMessagesResult.Succeeded)
					poisonQueueMessages = poisonQueueMessagesResult.Output;

				if (poisonQueueMessages == null || poisonQueueMessages.Count == 0)
					break;

				foreach (CloudQueueMessage poisonQueueMessage in poisonQueueMessages)
				{
					CloudQueueMessage retryMessage = new CloudQueueMessage(poisonQueueMessage.AsString);

					OpResult retryEnqueueResult = await EnqueueMessageAsync(storageAccount, queue, retryMessage);

					if (retryEnqueueResult.Succeeded)
						messagesToDeleteFromPoisonQueue.Add(poisonQueueMessage);
				}

				OpResult deleteResult = await DeleteMessagesAsync(storageAccount, poisonQueue, messagesToDeleteFromPoisonQueue);

				// TODO Should we sleep the thread?
			}

			result.Succeeded = true;

			return result;
		}

		#endregion

		#region Storage Primitives

		public StorageCredentials GetStorageCredentials(string storageAccountName, string storageAccountKey)
		{
			StorageCredentials storageCredentials = new StorageCredentials(storageAccountName, storageAccountKey);

			return storageCredentials;
		}

		public CloudStorageAccount GetStorageAccount(StorageCredentials storageCredentials)
		{
			CloudStorageAccount storageAccount = new CloudStorageAccount(storageCredentials, true);

			return storageAccount;
		}

		public CloudStorageAccount GetStorageAccount(string connectionString)
		{
			CloudStorageAccount storageAccount;

			try
			{
				bool worked = CloudStorageAccount.TryParse(connectionString, out storageAccount);
			}
			catch (Exception ex)
			{
				// TODO log exception

				storageAccount = null;
			}

			return storageAccount;
		}

		#endregion

		#region Validation

		public Validation ValidateStorageAccount(CloudStorageAccount storageAccount)
		{
			Validation result = new Validation();

			if (storageAccount == null)
			{
				result.Message = $"{nameof(storageAccount)} was null, empty, or whitespace.";
				result.IsValid = false;
			}
			else
			{
				result.Message = $"{nameof(storageAccount)} valid.";
				result.IsValid = true;
			}

			return result;
		}

		public Validation ValidateQueue(CloudQueue queue)
		{
			Validation result = new Validation();

			if (queue == null)
			{
				result.Message = $"{nameof(queue)} was null.";
				result.IsValid = false;
			}
			else
			{
				result.Message = $"{nameof(queue)} valid.";
				result.IsValid = true;
			}

			return result;
		}

		public Validation ValidateQueueName(string queueName)
		{
			Validation result = new Validation();

			if (string.IsNullOrWhiteSpace(queueName))
			{
				result.Message = $"{nameof(queueName)} was null or whitespace.";
				result.IsValid = false;
			}
			else
			{
				result.Message = $"{nameof(queueName)} valid.";
				result.IsValid = true;
			}

			return result;
		}

		public Validation ValidateQueueMessage(string queueMessage)
		{
			Validation result = new Validation();

			if (string.IsNullOrWhiteSpace(queueMessage))
			{
				result.Message = $"{nameof(queueMessage)} was null, empty, or whitespace.";
				result.IsValid = false;
			}
			else if ((queueMessage.Length * sizeof(Char)) > 64000)
			{
				// Storage queue message length max is 64KiB
				result.Message = $"{nameof(queueMessage)} lwngth exceeds 64 KiB, which is the Azure storage queue max size.";
				result.IsValid = false;
			}
			else
			{
				result.Message = $"{nameof(queueMessage)} valid.";
				result.IsValid = true;
			}

			return result;
		}

		public Validation ValidateQueueMessage(CloudQueueMessage queueMessage)
		{
			Validation result = new Validation();

			if (queueMessage == null)
			{
				result.Message = $"{nameof(queueMessage)} was null.";
				result.IsValid = false;
			}
			else
			{
				result.Message = $"{nameof(queueMessage)} valid.";
				result.IsValid = true;
			}

			return result;
		}

		public Validation ValidateQueueMessages(IEnumerable<CloudQueueMessage> queueMessages)
		{
			Validation result = new Validation();

			if (queueMessages == null)
			{
				result.Message = $"{nameof(queueMessages)} was null.";
				result.IsValid = false;
			}
			else
			{
				result.Message = $"{nameof(queueMessages)} valid.";
				result.IsValid = true;
			}

			return result;
		}

		#endregion
	}
}