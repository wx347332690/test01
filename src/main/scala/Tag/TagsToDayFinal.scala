//package Tag
//
//import com.mysql.jdbc.Connection
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.NamespaceDescriptor
//import org.apache.hadoop.hbase.client.Admin
//import org.apache.hadoop.hbase.client.Connection
//import org.apache.hadoop.hbase.client.ConnectionFactory
//
//object TagsToDayFinal  {
//  def main(args: Array[String]): Unit = {
//    //1 读取配置文件 读取的方式，先读取默认的配置文件，在读取
//    //自定义的配置文件
//
//    Configuration conf=HBaseConfiguration.create();
//
//    //2 与Hbase建立连接
//
//    Connection connection=ConnectionFactory.createConnection(conf);
//
//    //3 进行具体的操作
//
//    Admin admin=connection.getAdmin();  //admin对象获得hbase的连接
//
//    //4 通过admin 在hbase中创建命名空间
//    //定义一个namespace的描述类，在该类中定义namespace的名字，并进行构建
//    NamespaceDescriptor namespaceDescriptor=NamespaceDescriptor.create("myns").build();
//
//    //5 执行创建命令
//    admin.createNamespace(namespaceDescriptor);
//  }
//
//}
