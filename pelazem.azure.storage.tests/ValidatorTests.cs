﻿using System;
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
		}
	}
}