using System;
using System.Collections.Generic;
using System.Text;

namespace pelazem.azure.storage
{
	public class StorageConfig
	{
		public string StorageAccountName { get; set; }
		public string StorageAccountKey { get; set; }

		public string BlobContainerName { get; set; }

		public bool IsValidForBlob()
		{
			bool isValid =
				!string.IsNullOrWhiteSpace(this.StorageAccountName)
				&&
				!string.IsNullOrWhiteSpace(this.BlobContainerName)
			;

			return isValid;
		}
	}
}
