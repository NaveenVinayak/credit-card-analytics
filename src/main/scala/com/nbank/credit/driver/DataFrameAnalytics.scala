package com.nbank.credit.driver

import com.nbank.credit.config.ProjectConfig
import com.nbank.credit.spark.SparkContextFactory
import com.nbank.credit.constants.Constants
import com.nbank.credit.utils.{CreditUtils, FileUtils}
import org.apache.spark.sql.Row

/**
  * Naaveen
  *
  * Main executor class
  */

object DataFrameAnalytics extends ProjectConfig {

  def main(args: Array[String]): Unit = {

    /* Create local spark session */
    val sparkSession = SparkContextFactory.getSparkSession("local")
    /* Read the CSV file and create a temp view */
    FileUtils.readAsCSV(creditCardFile, sparkSession).createOrReplaceTempView(CreditUtils.creditCardsData)

    /*Print out the sample data in the form of a table*/
    val data=CreditUtils.creditDF(sparkSession).take(10).foreach(println)
    val dataset=sparkSession.sql("select Loan_ID from creditCardsData where Loan_Status='Y'").take(10).foreach(println)

    /*Count the total no of records*/
    val noOfRecords=CreditUtils.creditCardsCounts(sparkSession)
    println(noOfRecords)

    /*FInd out the no of males and no of females*/
    val noOfMales=CreditUtils.noOfMalesOrFemales(Constants.male,sparkSession)
    val noOfFemales=CreditUtils.noOfMalesOrFemales(Constants.female,sparkSession)
    println(noOfMales)
    println(noOfFemales)


    /*FInd out the no of married and unmarried count*/
    val noOfMarried= CreditUtils.noOfPeopleMarriedOrUnmarried(Constants.yes, sparkSession)
    val noofUnmarried=CreditUtils.noOfPeopleMarriedOrUnmarried(Constants.no, sparkSession)
    println("married"+noOfMarried)


    /*Find out the count of people having 3+ dependents*/
    val oneDependentCounts=CreditUtils.dependentsCount(Constants.threeplus,sparkSession)
    println(oneDependentCounts)

    /*Find out the graduated and non grduated count*/
    val graduationCount=CreditUtils.graduationCount(Constants.graduate,sparkSession)
    val nonGraduatesCount=CreditUtils.graduationCount(Constants.nonGraduate,sparkSession)

    /*Find out the slef employed count*/
    val selfEmployedCount=CreditUtils.selfEmployedCount(Constants.yes,sparkSession)
    val nonSelfEmployedCount=CreditUtils.selfEmployedCount(Constants.no,sparkSession)

    /*find out the count of the people having 0 or 1 credit history */
    val oneCreditHistoryCount=CreditUtils.CreditHistoryCount(Constants.one,sparkSession)
    val zeroCreditHistoryCount=CreditUtils.CreditHistoryCount(Constants.zero,sparkSession)

    /*Find out the total no of loans approved*/
    val noOfApprovedLoans=CreditUtils.noOfApprovedLoan(Constants.approved,sparkSession)
    val noOfNotApprovedLoans=CreditUtils.noOfApprovedLoan(Constants.notApproved,sparkSession)

    /**
      * Total no of approved loan status
      * area could be Urban Rural or SemiUrban
      */

    val areaWiseCount = CreditUtils.approvedLoanAreaWiseCount(Constants.approved, Constants.urban,sparkSession);


    /* Analyse the total no of loans approved for graduate and non graduate */
    CreditUtils.getDataOfEducationLoanStatus(Constants.graduate,Constants.approved,sparkSession).show();

    /* Analyse the loans approval among the married and gender wise and store as a textfile*/
    CreditUtils.getDataOnGenderMarriedLoanStatus(Constants.male,Constants.yes,Constants.approved,sparkSession).repartition(1).rdd.saveAsTextFile("/user/naveen/result-text");

    /*Analyse the loans approval among the married and who have completed graduation and store as parquet file*/
    CreditUtils.getDataOfEducationMarriedLoanStatus(Constants.graduate,Constants.yes,Constants.approved,sparkSession).coalesce(2).write.parquet("/user/naveen/result-parquet")

    /*Analyse the loans approval among the self employed people area wise and store as Json  */
    CreditUtils.getDataOfPropertyAreaLoanStatus(Constants.rural,Constants.approved,sparkSession).write.json("/user/naveen/result-json");

    /*Analyse the loans approval among the self employed people area wise*/
    CreditUtils.getDataOfPropertyAreaSelfEmployedLoanStatus(Constants.urban,Constants.no,Constants.approved,sparkSession).show()

    /*Analyse the loans approval amongs the people's credit history area wise*/
    CreditUtils.getDataOfPropertyAreaCreditHistoryLoanStatus(Constants.semiurban,Constants.zero,Constants.approved,sparkSession).write.text("/user/naveen/result-text1")

    /*Analyse the loans approval for people's dependents area wise*/
    CreditUtils.getDataOfPropertyAreaDependentsLoanStatus(Constants.rural,Constants.two,Constants.approved,sparkSession).show;

    /*Analyse the loans approval in eduaction and dependents and create a temp view */
    CreditUtils.getDataOfEducationDependentsLoanStatus(Constants.graduate,Constants.threeplus,Constants.approved,sparkSession).createOrReplaceTempView("New table")

    /*Analyse the loans approval in gender,eduaction and dependents and display 10 of them*/
    CreditUtils.getDataOfEducationDependentsLoanStatusGender(Constants.graduate,Constants.one,Constants.approved,Constants.male,sparkSession).show(10)

    /*Analyse the loans approval in gender,eduaction,married and dependents*/
    CreditUtils.getDataOfEducationDependentsLoanStatusGenderMarried(Constants.graduate,Constants.one,Constants.approved,Constants.male,Constants.yes,sparkSession).write.csv("/user/naveen/CSV")

    /*Analyse the loans approval in gender,eduaction,married,self employed, credit history and dependents*/
    CreditUtils.getDataOfDependentsLoanStatusGenderMarriedSelfEmployedCreditHistory(Constants.graduate,Constants.one,Constants.approved,Constants.male,Constants.yes,Constants.no,Constants.zero,sparkSession).show(10)

    /*Analyse loans approval by providing different values for ender,eduaction,married,self employed, credit history,area and dependents*/
    CreditUtils.CreditCardAnalytics(Constants.male,Constants.yes,Constants.two,Constants.graduate,Constants.yes,Constants.one,Constants.urban,Constants.approved,sparkSession).show(10)


    val totalAmountamount=CreditUtils.totalAmountOfLoanApproved(sparkSession)
    println(totalAmountamount)


  }
}