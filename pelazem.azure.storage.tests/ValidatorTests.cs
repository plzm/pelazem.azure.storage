using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using pelazem.azure.storage;
using pelazem.util;

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

	}
}
