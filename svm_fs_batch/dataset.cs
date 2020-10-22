using System;
using System.Collections.Generic;
using System.Text;

namespace svm_fs_batch
{
    internal class dataset
    {
        internal List<(int fid, string alphabet, string dimension, string category, string source, string group, string member, string perspective, int alphabet_id, int dimension_id, int category_id, int source_id, int group_id, int member_id, int perspective_id)> dataset_headers = null;
        internal List<(int filename_index, int line_index, List<(string comment_header, string comment_value)> comment_columns/*, string comment_columns_hash*/)> dataset_comment_row_values = null;
        internal List<(int class_id, int example_id, int class_example_id, List<(string comment_header, string comment_value)> comment_columns, /*string comment_columns_hash,*/ List<(int fid, double fv)> feature_data/*, string feature_data_hash*/)> dataset_instance_list = null;

        //internal static void serialise(datax datax, string filename)
        //{
        //    IFormatter formatter = new BinaryFormatter();
        //    Stream stream = new FileStream(filename, FileMode.Create, FileAccess.Write, FileShare.None);
        //    formatter.Serialize(stream, datax);
        //    stream.Close();
        //}

        //internal void serialise(string filename)
        //{
        //    datax.serialise(this, filename);
        //}

        //internal static datax deserialise(string filename)
        //{
        //    IFormatter formatter = new BinaryFormatter();
        //    Stream stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read);
        //    datax datax = (datax)formatter.Deserialize(stream);
        //    stream.Close();
        //    return datax;
        //}
    }
}
