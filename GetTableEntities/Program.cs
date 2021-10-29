using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;

namespace GetTableEntities
{
    class Program
    {
        // In bytes
        private const int sizeOfDouble = 8;
        // In bytes
        private const int sizeOfInt = 4;

        static void Main(string[] args)
        {
            var storageUri = "";
            var accountName = "";
            var storageAccountKey = "";

            var serviceClient = new TableServiceClient(new Uri(storageUri),new TableSharedKeyCredential(accountName, storageAccountKey));

            string tableName = "OfficeSupplies1p2";
            var tableClient = serviceClient.GetTableClient(tableName);

            tableClient.CreateIfNotExists();

            //Query all office supply
            Pageable<OfficeSupplyEntity> queryResultsLINQ = tableClient.Query<OfficeSupplyEntity>();

            if (queryResultsLINQ.Count() == 0)
            {
                PopulateEntityIfNotExists(tableClient, queryResultsLINQ);
            }

            foreach (var entity in queryResultsLINQ)
            {
                var entitySize = GetEntitySize(entity);
                Console.WriteLine($"{entity.Product} : entitySize = {entitySize} Bytes");
            }

            Console.WriteLine($"The size of this table is {queryResultsLINQ.Select(x => GetEntitySize(x)).Sum()} Bytes");
            Console.ReadLine();
        }

        private static void PopulateEntityIfNotExists(TableClient tableClient, Pageable<OfficeSupplyEntity> queryResultsLINQ)
        {
            var entity1 = new OfficeSupplyEntity
            {
                PartitionKey = "1",
                RowKey = "1",
                Product = "Notebook",
                Price = 3.00,
                Quantity = 50
            };

            var entity2 = new OfficeSupplyEntity
            {
                PartitionKey = "2",
                RowKey = "2",
                Product = "PC Computer",
                Price = 15.00,
                Quantity = 100
            };
           
            tableClient.AddEntity(entity1);
            tableClient.AddEntity(entity2);          
        }

        /// <summary>
        /// 4bytes + Len(PartitionKey + RowKey) * 2 bytes + For - Each Property(8 bytes + Len(Property Name) * 2 bytes + Sizeof(.Net Property Type))
        /// https://techcommunity.microsoft.com/t5/azure-paas-blog/calculate-the-size-capacity-of-storage-account-and-it-services/ba-p/1064046
        /// </summary>
        /// <param name="entity"></param>
        /// <returns>Size of an entity in bytes</returns>
        private static int GetEntitySize(OfficeSupplyEntity entity)
        {
            const int sizeOfPrice = (8 + (5 * 2) + sizeOfDouble);
            const int sizeOfQuantity = (8 + (8 * 2) + sizeOfInt);
            const int sizeOfFixedHeader = 4;

            int sizeOfProduct = (8 + (6 * 2) + (entity.Product.Length * 2 + 4));
            int sizeOfKeys = (entity.PartitionKey.Length + entity.RowKey.Length) * 2;

            //In bytes
            var entitySize = sizeOfFixedHeader + sizeOfKeys + sizeOfProduct + sizeOfPrice + sizeOfQuantity;
            return entitySize;
        }

        public class OfficeSupplyEntity : ITableEntity
        {
            public string Product { get; set; }
            public double Price { get; set; }
            public int Quantity { get; set; }
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
            public DateTimeOffset? Timestamp { get; set; }
            public ETag ETag { get; set; }
        }
    }
}
