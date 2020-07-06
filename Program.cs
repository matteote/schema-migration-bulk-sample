using System;
using System.Collections;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.Data.SqlClient;
using System.Text.RegularExpressions;

namespace schema_migration_sample
{
    class Program
    {
        static void Main(string[] args)
        {
            //source environment
            string sourceServer = "localhost";
            string sourceDatabase = "mydatabase";
            string sourceSchema = "schema1"; //source schema
            string sourceUser = null;
            string sourcePassword = null;
            bool sourceIntegratedSecurity = true;

            //target environment
            string targetServer = "targetserver";
            string targetDatabase = "targetdb";
            string targetUser ="user";
            string targetPassword = "********";
            bool targetIntegratedSecurity = false;

            //regex for source schema references replacement (with the exception of sp_executesql when targeting dbo schema)
            Regex re = new Regex(string.Format(@"(?<!\.)(\[{0}\]\.)|({0}\.)(?!sp_executesql)", sourceSchema));

            //connect to source environment
            SqlConnectionStringBuilder scsbSource = new SqlConnectionStringBuilder();
            scsbSource.DataSource = sourceServer;
            scsbSource.InitialCatalog = sourceDatabase;
            scsbSource.UserID = sourceUser;
            scsbSource.Password = sourcePassword;
            scsbSource.IntegratedSecurity = sourceIntegratedSecurity;
            var connSource = new SqlConnection(scsbSource.ToString());
            connSource.Open();
            var cmdSource = connSource.CreateCommand();

            //connect to target environment
            SqlConnectionStringBuilder scsbTarget = new SqlConnectionStringBuilder();
            scsbTarget.DataSource = targetServer;
            scsbTarget.InitialCatalog = targetDatabase;
            scsbTarget.UserID = targetUser;
            scsbTarget.Password = targetPassword;
            scsbTarget.IntegratedSecurity = targetIntegratedSecurity;
            var connTarget = new SqlConnection(scsbTarget.ToString());
            connTarget.Open();
            var cmdTarget = connTarget.CreateCommand();

            //get the default schema of the target user
            cmdTarget.CommandText = "select quotename(schema_name()) + '.'";
            string targetSchema = cmdTarget.ExecuteScalar() as string;

            //connect to local server via SMO
            Server srv = new Server(new ServerConnection(connSource));

            Log(string.Format("Connected to server {0}", srv.Name));

            //source database
            Database db = srv.Databases[sourceDatabase];

            Transfer transfer = new Transfer(db);

            /*
            SCRIPT THE TABLES
            */
            //populate a list of the tables in the source schema
            ArrayList objects = new ArrayList();
            foreach (Table t in db.Tables)
            {
                if (t.Schema.ToLower() == sourceSchema)
                {
                    Log(string.Format("Adding table {0}", t.Name));
                    objects.Add(t);
                }
            }
            //transfer options
            transfer.Options.TargetDatabaseEngineType = DatabaseEngineType.SqlAzureDatabase;
            transfer.CopyAllObjects = false;
            transfer.ObjectList = objects;
            transfer.CopyData = false;
            transfer.CopySchema = true;
            transfer.Options.ClusteredIndexes = true;
            transfer.Options.DriPrimaryKey = false;
            transfer.Options.DriChecks = false;
            transfer.Options.SchemaQualify = false;
            //run the scripts individually (workaround for lack of support of Transfer object for Azure SQL Database)
            foreach (var command in transfer.ScriptTransfer())
            {
                var cmd = re.Replace(command,targetSchema);
                Log(string.Format(">>> {0}",cmd));
                cmdTarget.CommandText = cmd;
                cmdTarget.ExecuteNonQuery();
            }

            /*
            TRANSFER THE DATA
            */
            foreach (Table t in objects)
            {
                cmdSource.CommandText = string.Format("SELECT * FROM [{0}].[{1}]", t.Schema, t.Name);
                var dataReader = cmdSource.ExecuteReader();

                SqlBulkCopy bcp = new SqlBulkCopy(connTarget);
                bcp.DestinationTableName = t.Name;
                bcp.WriteToServer(dataReader);
                dataReader.Close();
            }

            /*
            SCRIPT THE ENTIRE DATABASE
            */
            //populate a list of objects belonging to the source schema (except system objects)
            objects = new ArrayList();
            foreach (var urn in db.Schemas[sourceSchema].EnumOwnedObjects())
            {
                var obj = srv.GetSmoObject(urn);
                if ((bool)obj.Properties["IsSystemObject"].Value == false) {
                    Log(string.Format("Adding object {0}", urn));
                    objects.Add(obj);
                }
            }
            //transfer options
            transfer.Options.TargetDatabaseEngineType = DatabaseEngineType.SqlAzureDatabase;
            transfer.ObjectList = objects;
            transfer.Options.Indexes = true;
            transfer.Options.DriAll = true;
            transfer.Options.SchemaQualify = false;
            transfer.Options.IncludeIfNotExists = true;
            foreach (var command in transfer.ScriptTransfer())
            {
                var cmd = re.Replace(command,targetSchema);
                Log(string.Format(">>> {0}",cmd));
                cmdTarget.CommandText = cmd;
                cmdTarget.ExecuteNonQuery();
            }
        }

        static void Log(string message)
        {
            Console.WriteLine(message);
        }
    }
}
