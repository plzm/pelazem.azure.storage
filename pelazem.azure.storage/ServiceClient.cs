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

// Reference https://docs.microsoft.com/en-us/azure/storage/blobs/storage-dotnet-shared-access-signature-part-2

namespace pelazem.azure.storage
{
	public class ServiceClient
	{
		#region Get Blob URL

		public async Task<string> GetBlobUrlFromBlobPathAsync(StorageConfig storageConfig, string blobPath)
		{
			if (string.IsNullOrWhiteSpace(blobPath))
				return string.Empty;

			CloudBlob blob = await this.GetBlobFromPathAsync(storageConfig, blobPath);

			return blob.Uri.AbsoluteUri;
		}

		public async Task<string> GetBlobSAPUrlFromBlobPathAsync(StorageConfig storageConfig, string blobPath, string sharedAccessPolicyName)
		{
			string result = string.Empty;

			if (string.IsNullOrWhiteSpace(blobPath))
				return result;

			CloudBlob blob = await this.GetBlobFromPathAsync(storageConfig, blobPath);

			CloudBlobContainer container = await this.GetContainerAsync(storageConfig, true);

			SharedAccessBlobPolicy policy = await this.GetSharedAccessPolicy(container, sharedAccessPolicyName);

			if (policy != null)
				result = blob.Uri.AbsoluteUri + blob.GetSharedAccessSignature(policy);

			return result;
		}

		public async Task<string> GetBlobSAPUrlFromBlobAsync(ICloudBlob blob, string sharedAccessPolicyName)
		{
			string result = string.Empty;

			if (blob == null)
				return result;

			SharedAccessBlobPolicy policy = await this.GetSharedAccessPolicy(blob.Container, sharedAccessPolicyName);

			if (policy != null)
				result = blob.Uri.AbsoluteUri + blob.GetSharedAccessSignature(policy);

			return result;
		}

		public async Task<string> GetBlobSAPUrlFromBlobUrlAsync(StorageConfig storageConfig, string blobUrl, string sharedAccessPolicyName)
		{
			ICloudBlob blob = await GetBlobFromUrlAsync(storageConfig, blobUrl);

			string sapUrl = await GetBlobSAPUrlFromBlobAsync(blob, sharedAccessPolicyName);

			return sapUrl;
		}

		public async Task<string> GetBlobSASUrlFromBlobPathAsync(StorageConfig storageConfig, string blobPath, DateTimeOffset expiryDateTime, SharedAccessBlobPermissions policyPermissions = SharedAccessBlobPermissions.Read)
		{
			string result = string.Empty;

			if (string.IsNullOrWhiteSpace(blobPath))
				return result;

			CloudBlob blob = await this.GetBlobFromPathAsync(storageConfig, blobPath);

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
			CloudBlob blob = container.GetBlobReference(targetBlobPath);

			return blob;
		}

		public async Task<CloudBlob> GetBlobFromPathAsync(StorageConfig storageConfig, string targetBlobPath)
		{
			CloudBlobContainer container = await this.GetContainerAsync(storageConfig, true);

			CloudBlob blob = this.GetBlobFromPath(container, targetBlobPath);

			return blob;
		}

		public async Task<ICloudBlob> GetBlobFromUrlAsync(StorageConfig storageConfig, string blobUrl)
		{
			StorageCredentials storageCredentials = this.GetStorageCredentials(storageConfig);
			CloudStorageAccount storageAccount = this.GetStorageAccount(storageCredentials);
			CloudBlobClient blobClient = this.GetBlobClient(storageAccount);

			ICloudBlob result = await blobClient.GetBlobReferenceFromServerAsync(new Uri(blobUrl));

			return result;
		}

		#endregion

		#region List Blobs

		public async Task<IEnumerable<ICloudBlob>> ListBlobs(StorageConfig storageConfig)
		{
			CloudBlobContainer container = await this.GetContainerAsync(storageConfig, true);

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

		public async Task<bool> UploadFileFromUrlAsync(StorageConfig storageConfig, string sourceFileUrl, string targetBlobPath)
		{
			if (string.IsNullOrWhiteSpace(sourceFileUrl) || string.IsNullOrWhiteSpace(targetBlobPath))
				return false;

			bool result = false;

			try
			{
				WebRequest request = WebRequest.Create(sourceFileUrl);

				request.Timeout = 30000; // 30 seconds
				request.UseDefaultCredentials = true;
				request.Proxy.Credentials = request.Credentials;

				using (WebResponse response = await request.GetResponseAsync())
				{
					using (Stream stream = response.GetResponseStream())
					{
						result = await this.UploadStreamAsync(storageConfig, stream, targetBlobPath);
					}
				}
			}
			catch (Exception ex)
			{
				// TODO log exception

				result = false;
			}

			return result;
		}

		public async Task<bool> UploadFileFromLocalAsync(StorageConfig storageConfig, string sourceFilePath, string targetBlobPath)
		{
			if
			(
				string.IsNullOrWhiteSpace(sourceFilePath) ||
				string.IsNullOrWhiteSpace(targetBlobPath) ||
				!File.Exists(sourceFilePath)
			)
				return false;

			bool result = false;

			try
			{
				using (FileStream stream = File.OpenRead(sourceFilePath))
				{
					result = await this.UploadStreamAsync(storageConfig, stream, targetBlobPath);
				}
			}
			catch (Exception ex)
			{
				// TODO log exception

				result = false;
			}

			return result;
		}

		public async Task<bool> UploadStringAsync(StorageConfig storageConfig, string contents, string targetBlobPath)
		{
			byte[] bytes = Encoding.UTF8.GetBytes(contents);

			return await UploadByteArrayAsync(storageConfig, bytes, targetBlobPath);
		}

		public async Task<bool> UploadStreamAsync(StorageConfig storageConfig, Stream sourceStream, string targetBlobPath)
		{
			if (sourceStream == null || sourceStream.Length == 0)
				return false;

			if (string.IsNullOrWhiteSpace(targetBlobPath))
				return false;

			bool result = false;

			try
			{
				byte[] bytes;

				using (var memoryStream = new MemoryStream())
				{
					await sourceStream.CopyToAsync(memoryStream);

					bytes = memoryStream.ToArray();
				}

				result = await this.UploadByteArrayAsync(storageConfig, bytes, targetBlobPath);
			}
			catch (Exception ex)
			{
				// TODO log exception

				result = false;
			}

			return result;
		}

		public async Task<bool> UploadByteArrayAsync(StorageConfig storageConfig, byte[] bytes, string targetBlobPath)
		{
			if (bytes == null || bytes.Length == 0)
				return false;

			if (string.IsNullOrWhiteSpace(targetBlobPath))
				return false;

			bool result = false;

			try
			{
				CloudBlobContainer container = await this.GetContainerAsync(storageConfig, true);

				CloudBlockBlob blob = container.GetBlockBlobReference(targetBlobPath);

				// Upload the file
				await blob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);

				// Result is success if the blob exists - the upload operation does not return a status so we check success after the upload
				result = await blob.ExistsAsync();
			}
			catch (Exception ex)
			{
				// TODO log exception

				result = false;
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

		#region Storage Primitives

		public StorageConfig GetStorageConfig(string storageAccountName, string storageAccountKey, string containerName)
		{
			return new StorageConfig() { StorageAccountName = storageAccountName, StorageAccountKey = storageAccountKey, ContainerName = containerName };
		}

		public StorageCredentials GetStorageCredentials(StorageConfig storageConfig)
		{
			StorageCredentials storageCredentials = new StorageCredentials(storageConfig.StorageAccountName, storageConfig.StorageAccountKey);

			return storageCredentials;
		}

		public CloudStorageAccount GetStorageAccount(StorageCredentials storageCredentials)
		{
			CloudStorageAccount storageAccount = new CloudStorageAccount(storageCredentials, true);

			return storageAccount;
		}

		public CloudBlobClient GetBlobClient(CloudStorageAccount storageAccount)
		{
			CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

			return blobClient;
		}

		public async Task<CloudBlobContainer> GetContainerAsync(StorageConfig storageConfig, bool createIfNotExists, BlobContainerPublicAccessType publicAccessIfCreating = BlobContainerPublicAccessType.Off)
		{
			StorageCredentials storageCredentials = this.GetStorageCredentials(storageConfig);
			CloudStorageAccount storageAccount = this.GetStorageAccount(storageCredentials);
			CloudBlobClient blobClient = this.GetBlobClient(storageAccount);

			CloudBlobContainer result = null;

			try
			{
				result = blobClient.GetContainerReference(storageConfig.ContainerName);

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
	}
}