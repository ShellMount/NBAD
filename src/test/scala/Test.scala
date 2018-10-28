
import scala.util.control.Breaks._

object Test {
  def main(args: Array[String]): Unit = {


    breakable(

      for(i<-0 until 10) {

        println(i)

        if(i==5){

          break()

        }

      }

    )
    println("=====================")

    for(i<-0 until 10){

      breakable{

        if(i==3||i==6) {

          break

        }

        println(i)

      }

    }
  }
}
