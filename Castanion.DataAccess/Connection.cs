using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Castanion.DataAccess
{
    public class Connection
    {

        /// <summary>
        /// Create a new connection
        /// </summary>
        /// <param name="connectionString">Connection string base</param>
        /// <param name="commandType">Type of command, Stored procedure or Query</param>
        /// <param name="query">Name of stored procedure or string query to execute</param>
        /// <param name="transaction">Contains transacion to Sql</param>
        /// <returns></returns>
        public static SqlCommand GetConnection(CommandType commandType, String query, String connectionString, SqlTransaction transaction=null)
        {
            try
            {
                SqlCommand Command;
                SqlConnection Connection = new SqlConnection(connectionString);
                Command = new SqlCommand(query, Connection);
                Command.CommandType = commandType;

                if (transaction != null)
                    Command.Transaction = transaction;

                return Command;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        /// <summary>
        /// Return an object of type Datatable as result of query
        /// </summary>
        /// <param name="sqlCommand">SqlCommand Object</param>
        /// <returns></returns>
        public static DataTable GetDataTable(SqlCommand sqlCommand)
        {
            DataTable dt = new DataTable();
            try
            {
                sqlCommand.Connection.Open();
                SqlDataAdapter adapter = new SqlDataAdapter();
                adapter.SelectCommand = sqlCommand;
                adapter.Fill(dt);

                sqlCommand.Connection.Dispose();
                sqlCommand.Dispose();
            }
            catch (Exception ex)
            {
                sqlCommand.Connection.Dispose();
                sqlCommand.Dispose();
                throw ex;
            }


            return dt;
        }

        /// <summary>
        /// Return an object of type Dataset as result of query
        /// </summary>
        /// <param name="sqlCommand">SqlCommand Object</param>
        /// <returns></returns>
        public static DataSet GetDataSet(SqlCommand sqlCommand)
        {
            DataSet ds = new DataSet();
            try
            {
                sqlCommand.Connection.Open();
                SqlDataAdapter adapter = new SqlDataAdapter();
                adapter.SelectCommand = sqlCommand;
                adapter.Fill(ds);

                sqlCommand.Connection.Dispose();
                sqlCommand.Dispose();
            }
            catch (Exception ex)
            {
                sqlCommand.Connection.Dispose();
                sqlCommand.Dispose();
                throw ex;
            }

            return ds;
        }

        /// <summary>
        /// Execute query without result set, only true or false
        /// </summary>
        /// <param name="Command"></param>
        /// <returns></returns>
        public static bool ExecuteNonQuery(SqlCommand Command)
        {
            bool result = false;
            try
            {
                Command.Connection.Open();
                
                result= Command.ExecuteNonQuery() > 0;

                Command.Connection.Dispose();
                Command.Dispose();
            }
            catch (Exception ex)
            {
                Command.Connection.Dispose();
                Command.Dispose();

                throw ex;
            }

            return result;
        }
    }
}
