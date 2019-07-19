using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using pelazem.util;

namespace pelazem.azure.storage
{
	public class Queue
	{
		public async Task<OpResult<CloudQueue>> GetQueueAsync(CloudStorageAccount storageAccount, string queueName, bool createIfNotExists = true)
		{
			OpResult<CloudQueue> result = new OpResult<CloudQueue>();

			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateStorageAccount(storageAccount).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueueName(queueName).Validations);

			if (!(result.ValidationResult.IsValid))
				return result;

			try
			{
				CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

				CloudQueue queue = queueClient.GetQueueReference(queueName);

				if (createIfNotExists)
					await queue.CreateIfNotExistsAsync();

				result.Output = queue;
				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Exception = ex;
				result.Output = null;
				result.Succeeded = false;
			}

			return result;
		}

		/// <summary>
		/// Wraps https://docs.microsoft.com/en-us/dotnet/api/microsoft.windowsazure.storage.queue.cloudqueue.getmessagesasync.
		/// 
		/// NOTE: this retrieves messages but DOES NOT DELETE them from the queue. The retrieved messages are marked invisible for the default invisibility timeout.
		/// You - YES YOU - MUST explicitly delete messages off the queue so they do not "reappear".
		/// </summary>
		/// <param name="storageAccount"></param>
		/// <param name="queueName"></param>
		/// <param name="messageCount">Between 1 and 32. If outside this range, will be corrected internally to 32.</param>
		/// <returns></returns>
		public async Task<OpResult<List<CloudQueueMessage>>> GetQueueMessagesAsync(CloudStorageAccount storageAccount, CloudQueue queue, int messageCount = 32)
		{
			if (messageCount <= 0 || messageCount > 32)
				messageCount = 32;

			OpResult<List<CloudQueueMessage>> result = new OpResult<List<CloudQueueMessage>>();

			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateStorageAccount(storageAccount).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueue(queue).Validations);

			if (!(result.ValidationResult.IsValid))
				return result;

			try
			{
				result.Output = (await queue.GetMessagesAsync(messageCount)).ToList();
				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Succeeded = false;
				result.Exception = ex;
			}

			return result;
		}

		public async Task<OpResult> EnqueueMessageAsync(CloudStorageAccount storageAccount, CloudQueue queue, string queueMessage, TimeSpan? timeToLiveOnQueue = null, TimeSpan? timeBeforeVisible = null)
		{
			OpResult result = new OpResult();

			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateStorageAccount(storageAccount).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueue(queue).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueueMessage(queueMessage).Validations);

			if (!result.ValidationResult.IsValid)
				return result;

			CloudQueueMessage msg = new CloudQueueMessage(queueMessage);

			return await EnqueueMessageAsync(storageAccount, queue, msg, timeToLiveOnQueue, timeBeforeVisible);
		}

		public async Task<OpResult> EnqueueMessageAsync(CloudStorageAccount storageAccount, CloudQueue queue, CloudQueueMessage queueMessage, TimeSpan? timeToLiveOnQueue = null, TimeSpan? timeBeforeVisible = null)
		{
			OpResult result = new OpResult();

			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateStorageAccount(storageAccount).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueue(queue).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueueMessage(queueMessage).Validations);

			if (!result.ValidationResult.IsValid)
				return result;

			try
			{
				await queue.AddMessageAsync(queueMessage, timeToLiveOnQueue, timeBeforeVisible, null, null);

				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Succeeded = false;
				result.Exception = ex;
			}

			return result;
		}

		public async Task<OpResult> DeleteMessageAsync(CloudStorageAccount storageAccount, CloudQueue queue, CloudQueueMessage queueMessage)
		{
			OpResult result = new OpResult() { Succeeded = false };

			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateStorageAccount(storageAccount).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueue(queue).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueueMessage(queueMessage).Validations);

			if (!result.ValidationResult.IsValid)
				return result;

			try
			{
				await queue.DeleteMessageAsync(queueMessage);

				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Succeeded = false;
				result.Exception = ex;
			}

			return result;
		}

		public async Task<OpResult> DeleteMessagesAsync(CloudStorageAccount storageAccount, CloudQueue queue, IEnumerable<CloudQueueMessage> queueMessages)
		{
			OpResult result = new OpResult() { Succeeded = false };

			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateStorageAccount(storageAccount).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueue(queue).Validations);
			result.ValidationResult.Validations.AddItems<Validation>(Validator.ValidateQueueMessages(queueMessages).Validations);

			if (!result.ValidationResult.IsValid)
				return result;

			try
			{
				foreach (CloudQueueMessage queueMessage in queueMessages.Where(qm => qm != null))
					await queue.DeleteMessageAsync(queueMessage);

				result.Succeeded = true;
			}
			catch (Exception ex)
			{
				result.Succeeded = false;
				result.Exception = ex;
			}

			return result;
		}

		/// <summary>
		/// Removes all messages from the poison queue and tries them again. Processes batches of size batchSize.
		/// </summary>
		/// <param name="storageAccount"></param>
		/// <param name="queueName">The real queue - NOT the poison queue</param>
		/// <param name="batchSize">Number of messages to process at a time. Between 1 and 32.</param>
		/// <returns></returns>
		public async Task<OpResult> RetryPoisonQueueMessagesAsync(CloudStorageAccount storageAccount, string queueName, int batchSize)
		{
			OpResult result = new OpResult();

			OpResult<CloudQueue> queueResult = await this.GetQueueAsync(storageAccount, queueName, false);

			result.ValidationResult.Validations.AddItems(queueResult.ValidationResult.Validations);

			if (!result.ValidationResult.IsValid)
				return result;

			CloudQueue queue = null;

			if (queueResult.Succeeded && queueResult.Output != null)
				queue = queueResult.Output;
			else
			{
				result.Message = "Queue not found.";
				return result;
			}

			string poisonQueueName = queueName + "-poison";
			CloudQueue poisonQueue = null;

			OpResult<CloudQueue> poisonQueueResult = await this.GetQueueAsync(storageAccount, poisonQueueName, false);

			if (poisonQueueResult.Succeeded && poisonQueueResult.Output != null)
				poisonQueue = poisonQueueResult.Output;
			else
			{
				result.Message = "Poison Queue not found.";
				return result;
			}

			// We now have the queue and the poison queue
			List<CloudQueueMessage> messagesToDeleteFromPoisonQueue = new List<CloudQueueMessage>();

			while (true)
			{
				messagesToDeleteFromPoisonQueue.Clear();

				List<CloudQueueMessage> poisonQueueMessages = null;

				OpResult<List<CloudQueueMessage>> poisonQueueMessagesResult = await this.GetQueueMessagesAsync(storageAccount, poisonQueue, batchSize);

				if (poisonQueueMessagesResult.Succeeded)
					poisonQueueMessages = poisonQueueMessagesResult.Output;

				if (poisonQueueMessages == null || poisonQueueMessages.Count == 0)
					break;

				foreach (CloudQueueMessage poisonQueueMessage in poisonQueueMessages)
				{
					CloudQueueMessage retryMessage = new CloudQueueMessage(poisonQueueMessage.AsString);

					OpResult retryEnqueueResult = await this.EnqueueMessageAsync(storageAccount, queue, retryMessage);

					if (retryEnqueueResult.Succeeded)
						messagesToDeleteFromPoisonQueue.Add(poisonQueueMessage);
				}

				OpResult deleteResult = await this.DeleteMessagesAsync(storageAccount, poisonQueue, messagesToDeleteFromPoisonQueue);

				// TODO Should we sleep the thread?
			}

			result.Succeeded = true;

			return result;
		}
	}
}
