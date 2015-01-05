import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scaldual.core.LingualJob
import org.specs.Specification
import org.specs.mock.Mockito

class LingualJobTest extends Specification with Mockito {
  object TestData {
    val path = "docBOW.tsv"
    val input = Tsv(path, fields = new Fields("doc_id", "word", "count"))
    val scaldingInput = Tsv("scaldingBow.tsv", fields = new Fields("doc_id", "word", "count"))
    val data = List((1, "The", 5), (2, "Dog", 12), (3, "Car", 15))
  }

  abstract class TestJob(args:Args) extends LingualJob(args){
    output(Tsv("outputFile"))
  }

  "A Lingual Job" should {
    import TestData._
    "add tables correctly in lingual only job" in {
      val job = (args:Args) => {
        new TestJob(args) {
          table("bow", Tsv("docBOW.tsv", fields = ('doc_id, 'word, 'count)))
          override def query = """select "word", "count" from "bow""""
          override def cascadeComplete():Unit = {
            flowDef.getSources.size() must be_==(0)
            lingualFlow.getSinks.size() must be_==(1)
            lingualFlow.getSources.size() must be_==(1)
            outputSet must be_==(true)
          }
        }
      }

      JobTest(job).
        source(input, data).
        sink[(String, Int)](Tsv("outputFile")){outputBuffer => ()}.
        run.
        finish
    }

    "add tables correctly in scalding/lingual job" in {
      val job = (args:Args) => {
        new TestJob(args) {
          Tsv("scaldingBow.tsv", ('doc_id, 'word, 'count)).read.limit(1).write(Tsv("scaldingOutput"))
          table("bow", Tsv("docBOW.tsv", fields = ('doc_id, 'word, 'count)))
          override def query = """select "word", "count" from "bow""""
          override def cascadeComplete():Unit = {
            flowDef.getSources.size() must be_==(1)
            flowDef.getSinks.size() must be_==(1)
            lingualFlow.getSinks.size() must be_==(1)
            lingualFlow.getSources.size() must be_==(1)
          }
        }
      }

      JobTest(job).
        source(input, data).
        source(scaldingInput, List()).
        sink[(String, Int)](Tsv("outputFile")){outputBuffer => ()}.
        sink[(String, Int)](Tsv("scaldingOutput")){outputBuffer => ()}.
        run.
        finish
    }
  }
}