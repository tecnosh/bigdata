package bigdata.mba

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object PerfilEleitoral {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: PerfilEleitoral arquivo com extensao txt")
      System.exit(1)
    }

  val arquivo : String = args(0)

  val spark = SparkSession.builder()
	.master("local")
	.appName("Perfil Eleitoral")
	.config("spark.some.config.option", "some-value")
	.getOrCreate()

  import spark.implicits._

  val esquemaEleitor = StructType (
	StructField("periodo", StringType) ::
	StructField("uf", StringType) ::
	StructField("municipio", StringType) ::
	StructField("cod_municipio_tse", StringType) ::
	StructField("nr_zona", StringType) ::
	StructField("sexo", StringType) ::
	StructField("faixa_etaria", StringType) ::
	StructField("grau_de_escolaridade", StringType) ::
	StructField("qtd_eleitores_no_perf", IntegerType) :: Nil )

  val eleitorDF = spark.read.schema(esquemaEleitor)
        .options(Map("delimiter" -> ";", "header" -> "false"))
	.csv("./DadosEntrada/" + arquivo + ".txt")

  val esquemaUF_Regiao = StructType (
	StructField("uf", StringType) ::
	StructField("regiao", StringType) :: Nil )

  val uf_RegiaoDF = spark.read.schema(esquemaUF_Regiao)
        .options(Map("delimiter" -> ";", "header" -> "false"))
	.csv("./DadosEntrada/UF_Regiao.csv")

  eleitorDF.groupBy("periodo", "uf").sum("qtd_eleitores_no_perf")
	.write.mode("overwrite")
	.option("header", true)
	.csv("./DadosSaida/" + arquivo + "/uf")

  eleitorDF.groupBy("periodo", "faixa_etaria").sum("qtd_eleitores_no_perf")
	.write.mode("overwrite")
	.option("header", true)
	.csv("./DadosSaida/" + arquivo + "/faixa_etaria")

  eleitorDF.groupBy("periodo", "sexo").sum("qtd_eleitores_no_perf")
	.write.mode("overwrite")
	.option("header", true)
	.csv("./DadosSaida/" + arquivo + "/sexo")

  eleitorDF.groupBy("periodo", "grau_de_escolaridade").sum("qtd_eleitores_no_perf")
	.write.mode("overwrite")
	.option("header", true)
	.csv("./DadosSaida/" + arquivo + "/grau_de_escolaridade")

  eleitorDF.join(uf_RegiaoDF, eleitorDF("uf") === uf_RegiaoDF("uf"))
	.groupBy(eleitorDF("periodo"), uf_RegiaoDF("regiao"))
	.sum("qtd_eleitores_no_perf")
	.write.mode("overwrite")
	.option("header", true)
	.csv("./DadosSaida/" + arquivo + "/regiao")

  }
}
