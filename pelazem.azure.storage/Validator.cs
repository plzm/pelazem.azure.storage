using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Blob.Protocol;
using Microsoft.Azure.Storage.Queue;
using pelazem.util;

namespace pelazem.azure.storage
{
	public static class Validator
	{
		public static ValidationResult ValidateStorageAccount(CloudStorageAccount storageAccount)
		{
			ValidationResult result = new ValidationResult();

			if (storageAccount == null)
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(storageAccount)} was null." });

			if (result.Validations.Count == 0)
				result.Validations.Add(new Validation() { IsValid = true });

			return result;
		}

		#region Blob

		public static ValidationResult ValidateContainer(CloudBlobContainer container)
		{
			ValidationResult result = new ValidationResult();

			if (container == null)
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(container)} was null." });
			else
				result.Validations.AddItems(ValidateContainerName(container.Name).Validations);

			return result;
		}

		public static ValidationResult ValidateContainerName(string containerName)
		{
			return StorageNameValidatorWorker(containerName, NameValidator.ValidateContainerName);
		}

		public static ValidationResult ValidateBlobClient(CloudBlobClient blobClient)
		{
			ValidationResult result = new ValidationResult();

			if (blobClient == null)
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(blobClient)} was null." });

			if (result.Validations.Count == 0)
				result.Validations.Add(new Validation() { IsValid = true });

			return result;
		}

		public static ValidationResult ValidateBlob(ICloudBlob blob)
		{
			ValidationResult result = new ValidationResult();

			if (blob == null)
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(blob)} was null." });
			else
				result.Validations.AddItems(ValidateBlobPath(blob.Name).Validations);

			return result;
		}

		public static ValidationResult ValidateBlobPath(string blobPath)
		{
			return StorageNameValidatorWorker(blobPath, NameValidator.ValidateBlobName);
		}

		#endregion

		#region Queue

		public static ValidationResult ValidateQueue(CloudQueue queue)
		{
			ValidationResult result = new ValidationResult();

			if (queue == null)
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(queue)} was null." });

			if (result.Validations.Count == 0)
				result.Validations.Add(new Validation() { IsValid = true });

			return result;
		}

		public static ValidationResult ValidateQueueName(string queueName)
		{
			return StorageNameValidatorWorker(queueName, NameValidator.ValidateQueueName);
		}

		public static ValidationResult ValidateQueueMessage(string queueMessage)
		{
			ValidationResult result = new ValidationResult();

			if (string.IsNullOrWhiteSpace(queueMessage))
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(queueMessage)} was null, empty, or whitespace." });

			if ((!string.IsNullOrWhiteSpace(queueMessage)) && ((queueMessage.Length * sizeof(Char)) > 64000))
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(queueMessage)} length exceeds 64 KiB, which is the Azure storage queue max size." });

			if (result.Validations.Count == 0)
				result.Validations.Add(new Validation() { IsValid = true });

			return result;
		}

		public static ValidationResult ValidateQueueMessage(CloudQueueMessage queueMessage)
		{
			ValidationResult result = new ValidationResult();

			if (queueMessage == null)
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(queueMessage)} was null." });
			else
				result.Validations.AddItems(ValidateQueueMessage(queueMessage.AsString).Validations);

			return result;
		}

		public static ValidationResult ValidateQueueMessages(IEnumerable<CloudQueueMessage> queueMessages)
		{
			ValidationResult result = new ValidationResult();

			if (queueMessages == null)
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(queueMessages)} was null." });
			else if (queueMessages.Count() == 0)
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(queueMessages)} was empty." });
			else
				queueMessages.AsParallel().ForAll(qm => result.Validations.AddItems(ValidateQueueMessage(qm).Validations));

			return result;
		}

		#endregion

		public static ValidationResult ValidateFilePath(string filePath)
		{
			ValidationResult result = new ValidationResult();

			if (string.IsNullOrWhiteSpace(filePath))
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(filePath)} was null, empty, or whitespace." });

			if (!File.Exists(filePath))
				result.Validations.Add(new Validation() { IsValid = false, Message = $"Parameter {nameof(filePath)} file does not exist." });

			if (result.Validations.Count == 0)
				result.Validations.Add(new Validation() { IsValid = true });

			return result;
		}

		private static ValidationResult StorageNameValidatorWorker(string name, Action<string> nameValidatorMethod)
		{
			ValidationResult result = new ValidationResult();

			if (string.IsNullOrWhiteSpace(name))
				result.Validations.Add(new Validation() { IsValid = false, Message = $"{nameof(name)} was null, empty, or whitespace." });

			if (result.IsValid)
			{
				try
				{
					nameValidatorMethod(name);
				}
				catch (ArgumentException ex)
				{
					result.Validations.Add(new Validation() { IsValid = false, Message = $"{name} was invalid.{Environment.NewLine}{ex.Message}{Environment.NewLine}Please consult Azure Storage naming guidelines at https://docs.microsoft.com/rest/api/storageservices/Naming-and-Referencing-Containers--Blobs--and-Metadata." });
				}
			}

			if (result.Validations.Count == 0)
				result.Validations.Add(new Validation() { IsValid = true });

			return result;
		}
	}
}
