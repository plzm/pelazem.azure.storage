using System;
using System.Collections.Generic;
using System.Text;

namespace pelazem.azure.storage
{
	public struct StorageConfig
	{
		public string StorageAccountName { get; set; }
		public string StorageAccountKey { get; set; }

		public string ContainerName { get; set; }
	}
}
