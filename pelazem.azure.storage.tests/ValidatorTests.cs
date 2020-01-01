using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using pelazem.azure.storage;
using pelazem.util;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.Storage.Queue.Protocol;
using Microsoft.VisualStudio.TestPlatform.ObjectModel;
using Microsoft.Azure.Storage;

namespace pelazem.azure.storage.tests
{
	public class ValidatorTests
	{
		[Fact]
		public void ValidateStorageAccountShouldReturnFalseForNull()
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateStorageAccount(null);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateStorageAccountShouldReturnTrueForNonNull()
		{
			// Arrange
			StorageCredentials cred = new StorageCredentials("test", System.Text.Encoding.UTF8.GetBytes(" "));
			CloudStorageAccount sa = new CloudStorageAccount(cred, false);

			// Act
			ValidationResult validationResult = Validator.ValidateStorageAccount(sa);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.True(validationResult.Validations[0].IsValid);
			Assert.True(validationResult.IsValid);
		}

		[Fact]
		public void ValidateContainerShouldReturnFalseForNull()
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateContainer(null);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Theory]
		[InlineData(null)]
		[InlineData("")]
		public void ValidateContainerShouldReturnFalseForNullOrWhitespaceName(string value)
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateContainerName(value);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateBlobClientShouldReturnFalseForNull()
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateBlobClient(null);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateBlobShouldReturnFalseForNull()
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateBlob(null);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Theory]
		[InlineData(null)]
		[InlineData("")]
		public void ValidateBlobPathShouldReturnFalseForNullOrWhitespace(string value)
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateBlobPath(value);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateQueueShouldReturnFalseForNull()
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateQueue(null);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Theory]
		[InlineData(null)]
		[InlineData("")]
		public void ValidateQueueNameShouldReturnFalseForNullOrWhitespace(string value)
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateQueueName(value);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Theory]
		[InlineData(null)]
		[InlineData("")]
		[InlineData(" ")]
		public void ValidateQueueMessageShouldReturnFalseForNullOrWhitespace(string value)
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateQueueMessage(value);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateQueueMessageShouldReturnFalseForNull()
		{
			// Arrange
			CloudQueueMessage qm = null;

			// Act
			ValidationResult validationResult = Validator.ValidateQueueMessage(qm);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateQueueMessagesShouldReturnFalseForNull()
		{
			// Arrange
			List<CloudQueueMessage> queueMessages = null;

			// Act
			ValidationResult validationResult = Validator.ValidateQueueMessages(queueMessages);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateQueueMessagesShouldReturnFalseForEmpty()
		{
			// Arrange
			List<CloudQueueMessage> queueMessages = new List<CloudQueueMessage>();

			// Act
			ValidationResult validationResult = Validator.ValidateQueueMessages(queueMessages);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateQueueMessagesShouldReturnFalseForAtLeastOneInvalidMessage()
		{
			// Arrange
			List<CloudQueueMessage> queueMessages = new List<CloudQueueMessage>();
			queueMessages.Add(null);
			queueMessages.Add(new CloudQueueMessage("msg1", "rcpt1"));

			// Act
			ValidationResult validationResult = Validator.ValidateQueueMessages(queueMessages);

			// Assert
			Assert.Equal(queueMessages.Count, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.Validations[1].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Theory]
		[InlineData(null)]
		[InlineData("")]
		[InlineData(" ")]
		public void ValidateFilePathShouldReturnFalseForNullOrWhitespace(string value)
		{
			// Arrange

			// Act
			ValidationResult validationResult = Validator.ValidateFilePath(value);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}

		[Fact]
		public void ValidateFilePathShouldReturnFalseForNonexistentFileWithFilesystemCheck()
		{
			// Arrange
			string badFilePath = @"c:\foo\bar.txt";
			bool checkFileExistsInFileSystem = true;

			// Act
			ValidationResult validationResult = Validator.ValidateFilePath(badFilePath, checkFileExistsInFileSystem);

			// Assert
			Assert.Equal(1, validationResult.Validations.Count);
			Assert.False(validationResult.Validations[0].IsValid);
			Assert.False(validationResult.IsValid);
		}
	}
}
