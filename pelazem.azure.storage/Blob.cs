using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.XPath;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Queue;
using pelazem.util;

// Reference https://docs.microsoft.com/en-us/azure/storage/blobs/storage-dotnet-shared-access-signature-part-2

namespace pelazem.azure.storage
{
	public class Blob
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

			ICloudBlob blob = await this.GetBlobFromUrlAsync(storageAccount, blobUrl);

			result = await this.GetBlobSAPUrlFromBlobAsync(blob, sharedAccessPolicyName);

			return result;
		}

		public async Task<string> GetBlobSASUrlFromBlobPathAsync(CloudStorageAccount storageAccount, string containerName, string blobPath, DateTimeOffset expiryDateTime, SharedAccessBlobPermissions policyPermissions = SharedAccessBlobPermissions.Read)
		{
			string result = string.Empty;

			if (storageAccount == null || string.IsNullOrWhiteSpace(containerName) || string.IsNullOrWhiteSpace(blobPath))
				return result;

			CloudBlob blob = await this.GetBlobFromPathAsync(storageAccount, containerName, blobPath);

			SharedAccessBlobPolicy sasBlobPolicy = new SharedAccessBlobPolicy
			{
				SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5), // For clock skew
				SharedAccessExpiryTime = expiryDateTime,
				Permissions = policyPermissions
			};

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

			SharedAccessBlobPolicy sasBlobPolicy = new SharedAccessBlobPolicy
			{
				SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5), // For clock skew
				SharedAccessExpiryTime = expiryDateTime,
				Permissions = policyPermissions
			};

			//Generate the shared access signature on the blob, setting the constraints directly on the signature.
			string sasBlobToken = blob.GetSharedAccessSignature(sasBlobPolicy);

			result = blob.Uri.AbsoluteUri + sasBlobToken;

			return result;
		}

		#endregion

		#region Get Blob

		public CloudBlob GetBlobFromPath(CloudBlobContainer container, string targetBlobPath)
		{
			ValidationResult result = new ValidationResult();
			result.Validations.AddItems(Validator.ValidateContainer(container).Validations);
			result.Validations.AddItems(Validator.ValidateBlobPath(targetBlobPath).Validations);

			if (!result.IsValid)
				return null;

			CloudBlob blob = container.GetBlobReference(targetBlobPath);

			return blob;
		}

		public async Task<CloudBlob> GetBlobFromPathAsync(CloudStorageAccount storageAccount, string containerName, string targetBlobPath)
		{
			ValidationResult result = new ValidationResult();
			result.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			result.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);
			result.Validations.AddItems(Validator.ValidateBlobPath(targetBlobPath).Validations);

			if (!result.IsValid)
				return null;

			CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

			CloudBlob blob = this.GetBlobFromPath(container, targetBlobPath);

			return blob;
		}

		public async Task<ICloudBlob> GetBlobFromUrlAsync(CloudStorageAccount storageAccount, string blobUrl)
		{
			ValidationResult result = new ValidationResult();
			result.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);

			if (!result.IsValid)
				return null;

			CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

			ICloudBlob blob = await blobClient.GetBlobReferenceFromServerAsync(new Uri(blobUrl));

			return blob;
		}

		public async Task<Stream> GetBlobContentsAsync(ICloudBlob blob)
		{
			ValidationResult result = new ValidationResult();
			result.Validations.AddItems(Validator.ValidateBlob(blob).Validations);

			if (!result.IsValid)
				return null;

			Stream stream = new MemoryStream();

			await blob.DownloadToStreamAsync(stream);

			stream.Position = 0;

			return stream;
		}

		public async Task<string> GetBlobContentsAsync(Stream stream)
		{
			if (stream == null || !stream.CanRead)
				return string.Empty;

			string result = string.Empty;

			stream.Position = 0;

			using (StreamReader reader = new StreamReader(stream))
			{
				result = await reader.ReadToEndAsync();
			}

			return result;
		}

		/// <summary>
		/// Overload that ties blob content retrieval together and specifies string output
		/// </summary>
		/// <param name="storageAccountConnectionString"></param>
		/// <param name="blobUrl"></param>
		/// <returns></returns>
		public async Task<string> GetBlobContentsAsync(string storageAccountConnectionString, string blobUrl)
		{
			if (string.IsNullOrWhiteSpace(storageAccountConnectionString) || string.IsNullOrWhiteSpace(blobUrl))
				return string.Empty;

			CloudStorageAccount storageAccount = Common.GetStorageAccount(storageAccountConnectionString);

			ValidationResult validationResult = Validator.ValidateStorageAccount(storageAccount);

			if (!validationResult.IsValid)
				return null;

			string result = string.Empty;

			ICloudBlob b = await this.GetBlobFromUrlAsync(storageAccount, blobUrl);

			using (Stream contents = await this.GetBlobContentsAsync(b))
			{
				result = await this.GetBlobContentsAsync(contents);
			}

			return result;
		}

		#endregion

		#region List Blobs

		public async Task<IEnumerable<ICloudBlob>> ListBlobsAsync(CloudStorageAccount storageAccount, string containerName, string blobNamePrefix = "", int? maxResultsPerSegment = 50)
		{
			ValidationResult result = new ValidationResult();
			result.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			result.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);

			if (!result.IsValid)
				return null;

			CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

			List<ICloudBlob> blobs = new List<ICloudBlob>();

			BlobContinuationToken token = null;

			do
			{
				var response = await container.ListBlobsSegmentedAsync(blobNamePrefix, true, BlobListingDetails.Metadata, maxResultsPerSegment, token, null, null);

				token = response.ContinuationToken;

				blobs.AddRange(response.Results.Where(b => b is ICloudBlob).Select(b => b as ICloudBlob));
			}
			while (token != null);

			return blobs;
		}

		#endregion

		#region Upload Blob

		public async Task<OpResult> UploadFileFromUrlAsync(CloudStorageAccount storageAccount, string containerName, string sourceFileUrl, string targetBlobPath)
		{
			OpResult opResult = new OpResult() { Succeeded = false };

			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);
			validationResult.Validations.AddItems(Validator.ValidateBlobPath(targetBlobPath).Validations);

			opResult.ValidationResult = validationResult;

			if (!validationResult.IsValid)
				return opResult;

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
						opResult = await this.UploadStreamAsync(storageAccount, containerName, stream, targetBlobPath);
					}
				}
			}
			catch (Exception ex)
			{
				// TODO log exception

				opResult.Succeeded = false;
				opResult.Message = ErrorUtil.GetText(ex);
				opResult.Exception = ex;
			}

			return opResult;
		}

		public async Task<OpResult> UploadFileFromLocalAsync(CloudStorageAccount storageAccount, string containerName, string sourceFilePath, string targetBlobPath)
		{
			OpResult opResult = new OpResult() { Succeeded = false };

			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);
			validationResult.Validations.AddItems(Validator.ValidateBlobPath(targetBlobPath).Validations);
			validationResult.Validations.AddItems(Validator.ValidateFilePath(sourceFilePath).Validations);

			opResult.ValidationResult = validationResult;

			if (!validationResult.IsValid)
				return opResult;

			try
			{
				using (FileStream stream = File.OpenRead(sourceFilePath))
				{
					opResult = await this.UploadStreamAsync(storageAccount, containerName, stream, targetBlobPath);
				}
			}
			catch (Exception ex)
			{
				// TODO log exception

				opResult.Succeeded = false;
				opResult.Message = ErrorUtil.GetText(ex);
				opResult.Exception = ex;
			}

			return opResult;
		}

		public async Task<OpResult> UploadStringAsync(CloudStorageAccount storageAccount, string containerName, string contents, string targetBlobPath)
		{
			byte[] bytes = Encoding.UTF8.GetBytes(contents);

			return await this.UploadByteArrayAsync(storageAccount, containerName, bytes, targetBlobPath);
		}

		public async Task<OpResult> UploadStreamAsync(CloudStorageAccount storageAccount, string containerName, Stream sourceStream, string targetBlobPath)
		{
			OpResult opResult = new OpResult() { Succeeded = false };

			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);
			validationResult.Validations.AddItems(Validator.ValidateBlobPath(targetBlobPath).Validations);

			opResult.ValidationResult = validationResult;

			if (!validationResult.IsValid)
				return opResult;

			try
			{
				byte[] bytes;

				using (var memoryStream = new MemoryStream())
				{
					await sourceStream.CopyToAsync(memoryStream);

					bytes = memoryStream.ToArray();
				}

				opResult = await this.UploadByteArrayAsync(storageAccount, containerName, bytes, targetBlobPath);
			}
			catch (Exception ex)
			{
				// TODO log exception

				opResult.Succeeded = false;
				opResult.Message = ErrorUtil.GetText(ex);
				opResult.Exception = ex;
			}

			return opResult;
		}

		public async Task<OpResult> UploadByteArrayAsync(CloudStorageAccount storageAccount, string containerName, byte[] bytes, string targetBlobPath)
		{
			OpResult opResult = new OpResult() { Succeeded = false };

			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);
			validationResult.Validations.AddItems(Validator.ValidateBlobPath(targetBlobPath).Validations);

			opResult.ValidationResult = validationResult;

			if (!validationResult.IsValid)
				return opResult;

			try
			{
				CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

				CloudBlockBlob blob = container.GetBlockBlobReference(targetBlobPath);

				// Upload the file
				await blob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);

				// Result is success if the blob exists - the upload operation does not return a status so we check success after the upload
				opResult.Succeeded = await blob.ExistsAsync();
			}
			catch (Exception ex)
			{
				// TODO log exception

				opResult.Succeeded = false;
				opResult.Message = ErrorUtil.GetText(ex);
				opResult.Exception = ex;
			}

			return opResult;
		}

		#endregion

		#region Blob Operations

		/// <summary>
		/// Starts async copy of source to target block blob.
		/// ********** NOTE that block blob copy operations are ASYNCHRONOUS - just returning from this method does NOT mean the copy has already finished.
		/// Returns the target block blob so you can optionally check or wait on targetBlob.CopyState.Status to transition from Pending.
		/// Source and target storage accounts and containers can be the same, or different for greater flexibility.
		/// </summary>
		/// <param name="sourceStorageAccount"></param>
		/// <param name="sourceContainerName"></param>
		/// <param name="sourceBlobPathInContainer"></param>
		/// <param name="targetStorageAccount"></param>
		/// <param name="targetContainerName"></param>
		/// <param name="targetBlobPathInContainer"></param>
		/// <returns>CloudBlockBlob on OpResult.Output - cast Output to CloudBlockBlob to use</returns>
		public async Task<OpResult> CopyBlockBlobAsync(CloudStorageAccount sourceStorageAccount, string sourceContainerName, string sourceBlobPathInContainer, CloudStorageAccount targetStorageAccount, string targetContainerName, string targetBlobPathInContainer)
		{
			OpResult opResult = new OpResult() { Succeeded = false };

			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(sourceStorageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(sourceContainerName).Validations);
			validationResult.Validations.AddItems(Validator.ValidateBlobPath(sourceBlobPathInContainer).Validations);
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(targetStorageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(targetContainerName).Validations);
			validationResult.Validations.AddItems(Validator.ValidateBlobPath(targetBlobPathInContainer).Validations);

			opResult.ValidationResult = validationResult;

			if (!validationResult.IsValid)
				return opResult;

			// Cleanup
			// Blob paths should NOT have leading slashes
			if (sourceBlobPathInContainer.StartsWith("/"))
				sourceBlobPathInContainer = sourceBlobPathInContainer.Substring(1);
			if (targetBlobPathInContainer.StartsWith("/"))
				targetBlobPathInContainer = targetBlobPathInContainer.Substring(1);

			try
			{
				CloudBlobContainer sourceContainer = await GetContainerAsync(sourceStorageAccount, sourceContainerName, false);
				CloudBlobContainer targetContainer = await GetContainerAsync(targetStorageAccount, targetContainerName, true);

				CloudBlockBlob sourceBlob = sourceContainer.GetBlockBlobReference(sourceBlobPathInContainer);
				CloudBlockBlob targetBlob = targetContainer.GetBlockBlobReference(targetBlobPathInContainer);

				await targetBlob.StartCopyAsync(sourceBlob);

				opResult.Succeeded = true;
				opResult.Output = targetBlob;
			}
			catch (Exception ex)
			{
				opResult.Succeeded = false;
				opResult.Message = ErrorUtil.GetText(ex);
				opResult.Exception = ex;
			}

			return opResult;
		}

		/// <summary>
		/// Deletes a blob asynchronously.
		/// </summary>
		/// <param name="storageAccount"></param>
		/// <param name="containerName"></param>
		/// <param name="targetBlobPath"></param>
		/// <returns></returns>
		public async Task<OpResult> DeleteBlobByPathAsync(CloudStorageAccount storageAccount, string containerName, string targetBlobPath)
		{
			OpResult opResult = new OpResult();

			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);
			validationResult.Validations.AddItems(Validator.ValidateBlobPath(targetBlobPath).Validations);

			opResult.ValidationResult = validationResult;

			if (!validationResult.IsValid)
				return opResult;

			// Cleanup
			// Blob paths should NOT have leading slashes
			if (targetBlobPath.StartsWith("/"))
				targetBlobPath = targetBlobPath.Substring(1);

			CloudBlob blob = await this.GetBlobFromPathAsync(storageAccount, containerName, targetBlobPath);

			if (blob != null)
				try
				{
					await blob.DeleteIfExistsAsync();

					opResult.Succeeded = true;
				}
				catch (Exception ex)
				{
					opResult.Succeeded = false;
					opResult.Message = ErrorUtil.GetText(ex);
					opResult.Exception = ex;
				}
			else
			{
				opResult.Succeeded = false;
				opResult.Message = "Specified blob does not exist.";
			}

			return opResult;
		}

		public async Task<OpResult> WriteBlobAsync(string storageAccountConnectionString, string containerName, string fileContents, string filePath)
		{
			OpResult opResult = new OpResult();

			CloudStorageAccount storageAccount = Common.GetStorageAccount(storageAccountConnectionString);

			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);
			validationResult.Validations.AddItems(Validator.ValidateFilePath(filePath).Validations);

			opResult.ValidationResult = validationResult;

			if (!validationResult.IsValid)
				return opResult;

			// Serialize file contents to byte array
			byte[] bytes = Encoding.UTF8.GetBytes(fileContents);

			try
			{
				CloudBlobContainer container = await this.GetContainerAsync(storageAccount, containerName, true);

				CloudBlockBlob blob = container.GetBlockBlobReference(filePath);

				// Upload the file
				await blob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);

				// Result is success if the blob exists - the upload operation does not return a status so we check success after the upload
				opResult.Succeeded = await blob.ExistsAsync();
			}
			catch (Exception ex)
			{
				// TODO log error

				opResult.Succeeded = false;
				opResult.Message = ErrorUtil.GetText(ex);
				opResult.Exception = ex;
			}

			return opResult;
		}

		#endregion

		#region Shared Access Policy

		public async Task<SharedAccessBlobPolicy> GetSharedAccessPolicy(CloudBlobContainer container, string policyName)
		{
			ValidationResult validationResult = Validator.ValidateContainer(container);

			if (!validationResult.IsValid)
				return null;

			SharedAccessBlobPolicy policy = null;

			BlobContainerPermissions permissions = await container.GetPermissionsAsync();

			if (permissions.SharedAccessPolicies.ContainsKey(policyName))
				policy = permissions.SharedAccessPolicies.FirstOrDefault(p => p.Key == policyName).Value;

			return policy;
		}

		public async Task CreateSharedAccessPolicy(CloudBlobClient blobClient, CloudBlobContainer container, string policyName, DateTimeOffset? expiryDateTime, SharedAccessBlobPermissions policyPermissions = SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read)
		{
			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateBlobClient(blobClient).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainer(container).Validations);

			if (!validationResult.IsValid)
				return;

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
			ValidationResult validationResult = Validator.ValidateStorageAccount(storageAccount);

			if (!validationResult.IsValid)
				return null;

			return storageAccount.CreateCloudBlobClient();
		}

		public async Task<CloudBlobContainer> GetContainerAsync(CloudStorageAccount storageAccount, string containerName, bool createIfNotExists, BlobContainerPublicAccessType publicAccessIfCreating = BlobContainerPublicAccessType.Off)
		{
			ValidationResult validationResult = new ValidationResult();
			validationResult.Validations.AddItems(Validator.ValidateStorageAccount(storageAccount).Validations);
			validationResult.Validations.AddItems(Validator.ValidateContainerName(containerName).Validations);

			if (!validationResult.IsValid)
				return null;

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
	}
}