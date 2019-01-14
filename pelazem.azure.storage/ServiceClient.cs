using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
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

		public async Task<IEnumerable<ICloudBlob>> ListBlobs(CloudStorageAccount storageAccount, string containerName)
		{
			CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

			List<ICloudBlob> result = new List<ICloudBlob>();

			BlobContinuationToken token = null;

			do
			{
				var response = await container.ListBlobsSegmentedAsync(string.Empty, true, BlobListingDetails.Metadata, 50, token, null, null);

				token = response.ContinuationToken;

				foreach (var blob in response.Results.Where(b => b is ICloudBlob))
					result.Add(blob as ICloudBlob);
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

		public async Task<OpResult> Enqueue(CloudStorageAccount storageAccount, string queueName, string message, TimeSpan? timeToLiveOnQueue = null, TimeSpan? timeBeforeVisible = null)
		{
			OpResult result = new OpResult() { Succeeded = false };

			if (storageAccount == null)
			{
				result.Message = $"Parameter {nameof(storageAccount)} was null.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(queueName))
			{
				result.Message = $"Parameter {nameof(queueName)} was empty or whitespace.";
				return result;
			}

			if (string.IsNullOrWhiteSpace(message))
			{
				result.Message = $"Parameter {nameof(message)} was empty or whitespace.";
				return result;
			}

			CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

			CloudQueue queue = queueClient.GetQueueReference(queueName);

			try
			{
				await queue.CreateIfNotExistsAsync();

				await queue.AddMessageAsync(new CloudQueueMessage(message), timeToLiveOnQueue, timeBeforeVisible, null, null);

				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				// TODO Log Exception

				// TODO log exception

				result.Succeeded = false;
				result.Message = "Error! Exception caught. See Output property for Exception.";
				result.Output = ex;
			}

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
	}
}