using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;

namespace pelazem.azure.storage
{
	public class Common
	{
		#region Storage Primitives

		public static StorageCredentials GetStorageCredentials(string storageAccountName, string storageAccountKey)
		{
			StorageCredentials storageCredentials = new StorageCredentials(storageAccountName, storageAccountKey);

			return storageCredentials;
		}

		public static CloudStorageAccount GetStorageAccount(StorageCredentials storageCredentials)
		{
			CloudStorageAccount storageAccount = new CloudStorageAccount(storageCredentials, true);

			return storageAccount;
		}

		public static CloudStorageAccount GetStorageAccount(string connectionString)
		{
			CloudStorageAccount storageAccount;

			try
			{
				bool worked = CloudStorageAccount.TryParse(connectionString, out storageAccount);
			}
			catch
			{
				// TODO log exception

				storageAccount = null;
			}

			return storageAccount;
		}

		#endregion
	}
}
