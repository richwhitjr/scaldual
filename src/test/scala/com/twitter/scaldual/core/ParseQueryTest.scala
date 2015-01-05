import com.twitter.scaldual.core.ParseQuery
import org.specs.Specification
import org.specs.mock.Mockito

class ParseQueryTest extends Specification with Mockito {
  def testTables(tables:Seq[String], query:String) = tables must haveSameElementsAs(ParseQuery.tables(query))

  "A Parse Query Object" should {
    "find tables in queries" in {
      testTables(Seq("TABLE1", "TABLE2", "TABLE3"), "select * from table1, table2, table3")
      testTables(Seq("TABLE1", "TABLE2"), """select * from table1, table2 where table1 = table2 order by table2.A""")
      testTables(Seq("TABLE1", "TABLE2"), "select * from table1, table2 where table1 = table2 group by table2.A")
      testTables(Seq("TABLE1", "TABLE2"), "select * from table1, table2 where table1 = table2 group by table2 having table1.A > 1")
    }
  }
}