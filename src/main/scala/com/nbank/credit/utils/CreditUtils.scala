package com.nbank.credit.utils

import com.nbank.credit.config.ProjectConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CreditUtils extends ProjectConfig{
    val creditCardsData ="creditCardsData"
    val Loan_ID="Loan_ID"
    val Gender="Gender"
    val Married="Married"
    val Dependents="Dependents"
    val Education="Education"
    val Self_Employed="Self_Employed"
    val ApplicantIncome="ApplicantIncome"
    val CoapplicantIncome="CoapplicantIncome"
    val LoanAmount="LoanAmount"
    val Loan_Amount_Term="Loan_Amount_Term"
    val Credit_History="Credit_History"
    val Property_Area="Property_Area"
    val Loan_Status="Loan_Status"



  /**
    *  Get DataFrame of all credit cards
    */
  def creditDF(sparkSession: SparkSession): DataFrame = {
    sparkSession.sql("select * from " + creditCardsData)
  }

  def creditCardsCounts(sparkSession: SparkSession) :Long = {
    sparkSession.sql("select * from " + creditCardsData).count()
  }

  /**
	* Total no of males or females
	* gender could be Male or Female
	*/
	
	def noOfMalesOrFemales(gender:String,sparkSession:SparkSession):Long={
    sparkSession.sql("select * from "+creditCardsData+" where "+Gender+" ='"+gender+"'").count()
  }
  
    /**
	* Total no of married or married people
	* marry could be No or Yes
	*/
  	def noOfPeopleMarriedOrUnmarried(marry:String,sparkSession:SparkSession):Long={
    sparkSession.sql("select * from "+creditCardsData+" where "+Married+"='"+marry+"'").count()
  }

  /**
  *Dependents count 
  * count could be 0,1,2 and 3+
  */
  
    def dependentsCount(count:String,sparkSession:SparkSession):Long={
    sparkSession.sql("select * from "+creditCardsData+" where "+Dependents+"='"+count+"'").count()
  }
  
  /** 
  *No of people who are graduated and not graduated
  * graduation could be Graduate or Not Graduate
  */
    def graduationCount(graduation:String,sparkSession:SparkSession):Long={
    sparkSession.sql("select * from "+creditCardsData+" where "+Education+"='"+graduation+"'").count()
  }
  
    /**
	* Total no of people who are selfEmployed or Not 
	* selfEmployed could be No or Yes
	*/
    def selfEmployedCount(selfEmployed:String,sparkSession:SparkSession):Long={
    sparkSession.sql("select * from "+creditCardsData+" where "+Self_Employed+"='"+selfEmployed+"'").count()
  }
  
  /**
	* Credit History count
	* Credit History could be 0 or 1
	*/
	def CreditHistoryCount(creditHistory:String,sparkSession:SparkSession):Long={
    sparkSession.sql("select * from "+creditCardsData+" where "+Credit_History+"='"+creditHistory+"'").count()
  }
  
  /**
	* Total no of approved loan status
	* loan status could be N or Y
	*/
  	def noOfApprovedLoan(loanApproved:String,sparkSession:SparkSession):DataFrame={
    sparkSession.sql("select * from "+creditCardsData+" where "+Loan_Status+"='"+loanApproved+"'")
  }
  
  /**
	* Total no of approved loan status
	* area could be Urban Rural or SemiUrban
	*/
  	def approvedLoanAreaWiseCount(loanApproved:String, area:String,sparkSession:SparkSession):Long={
      sparkSession.sql("select * from "+creditCardsData+" where "+Loan_Status+"='"+loanApproved+"and "+Property_Area+"='"+area+"'").count()
  }

  /* Analyse the total no of loans approved for graduate and non graduate */
  def getDataOfEducationLoanStatus( education:String,loan_Status:String,sparkSession: SparkSession): DataFrame={
    sparkSession.sql("select Loan_ID from "+creditCardsData+" where Education='"+education+"' and Loan_Status='"
      +loan_Status+"'")
  }

  /* Analyse the loans approval among the married and gender wise */
  def getDataOnGenderMarriedLoanStatus(gender:String, married:String, loan_Status:String, sparkSession:SparkSession) : DataFrame={
    sparkSession.sql("select Loan_ID from "+creditCardsData+ " where Gender='"+gender+"' and Married='"+married+"' and Loan_Status='"+loan_Status+"'")
  }

  /*Analyse the loans approval among the married and who have completed graduation*/
  def getDataOfEducationMarriedLoanStatus(education:String,married: String,loan_Status: String ,sparkSession: SparkSession): DataFrame ={
    sparkSession.sql("select Loan_ID "+creditCardsData+" where Married='"+married+"' and education='"+education+"' and Loan_Status='"+loan_Status+"'")
  }

  /*Analyse the loans aproval area wise*/
  def getDataOfPropertyAreaLoanStatus(property_Area:String,loan_Status: String,sparkSession: SparkSession):DataFrame={
    sparkSession.sql("select Loan_ID "+creditCardsData+" where Property_Area='"+property_Area+"' and Loan_Status='"+loan_Status+"'")
  }

  /*Analyse the loans approval among the self employed people area wise*/
  def getDataOfPropertyAreaSelfEmployedLoanStatus(property_Area: String,selfEmployed: String,loan_Status: String,sparkSession: SparkSession):DataFrame={
    sparkSession.sql("select Loan_ID "+creditCardsData+" where Property_Area='"+property_Area+"' and selfEmployed='"+selfEmployed+"' and  Loan_Status='"+loan_Status+"'")
  }

  /*Analyse the loans approval amongs the people's credit history area wise*/
  def getDataOfPropertyAreaCreditHistoryLoanStatus(property_Area: String,creditHistory:String,loan_Status: String,sparkSession: SparkSession):DataFrame={
    sparkSession.sql("select Loan_ID "+creditCardsData+" where Property_Area='"+property_Area+"' and creditHistory='"+creditHistory+"' and  Loan_Status='"+loan_Status+"'")
  }

  /*Analyse the loans approval for people's dependents area wise*/
  def getDataOfPropertyAreaDependentsLoanStatus(property_Area: String,dependents: String,loan_Status: String,sparkSession: SparkSession):DataFrame={
    sparkSession.sql("select Loan_ID "+creditCardsData+" where Property_Area='"+property_Area+"' and Dependents='"+dependents+"' and  Loan_Status='"+loan_Status+"'")
  }

  /*Analyse the loans approval in eduaction and dependents*/
  def getDataOfEducationDependentsLoanStatus(education: String,dependents: String,loan_Status: String,sparkSession: SparkSession):DataFrame={
    sparkSession.sql("select Loan_ID "+creditCardsData+" where education='"+education+"' and Dependents='"+dependents+"' and  Loan_Status='"+loan_Status+"'")
  }

  /*Analyse the loans approval in gender,eduaction and dependents*/
  def getDataOfEducationDependentsLoanStatusGender(education: String,dependents: String,loan_Status: String, gender:String,sparkSession: SparkSession):DataFrame={

    sparkSession.sql("select Loan_ID "+creditCardsData+" where education='"+education+"' and Dependents='"+dependents+"' and Loan_Status='"+loan_Status+"' and Gender='"+gender+"'")
  }

  /*Analyse the loans approval in gender,eduaction,married and dependents*/
  def getDataOfEducationDependentsLoanStatusGenderMarried(education: String,dependents: String,loan_Status: String, gender:String,married: String,sparkSession: SparkSession):DataFrame={
    sparkSession.sql("select Loan_ID "+creditCardsData+" where Gender ='"+gender+"' and Married='"+married+"' and Dependents='"+dependents+"' and education='"+education+"' and Loan_Status='"+loan_Status+"'")
  }

  /*Analyse the loans approval in gender,eduaction,married,self employed, credit history and dependents*/
  def getDataOfDependentsLoanStatusGenderMarriedSelfEmployedCreditHistory(education: String,dependents: String,loan_Status: String, gender:String,married: String,selfEmployed:String,creditHistory:String,sparkSession: SparkSession):DataFrame={
    sparkSession.sql("select Loan_ID "+creditCardsData+" where Gender ='"+gender+"' and Married='"+married+"' and Dependents='"+dependents+"' and selfEmployed='"+selfEmployed+"' and creditHistory ='"+creditHistory+"' and Loan_Status='"+loan_Status+"'")
  }

  /*Analyse loans approval by providing different values for ender,eduaction,married,self employed, credit history,area and dependents*/
	def CreditCardAnalytics(gender:String, married:String, dependents:String, education:String, selfEmployed:String, creditHistory :String, property_Area:String, loan_Status:String, sparkSession:SparkSession)={
		sparkSession.sql("select Loan_ID "+creditCardsData+" where Gender ='"+gender+"' and Married='"+married+"' and Dependents='"+dependents+"' and education='"+education+"' and selfEmployed='"+selfEmployed+"' and creditHistory ='"+creditHistory+"' and Property_Area='"+property_Area+"' and Loan_Status='"+loan_Status+"'")
	}

  /*Find out the total approved loan amount*/
  def totalAmountOfLoanApproved(sparkSession: SparkSession) : Double={
    sparkSession.sql("select sum(LoanAmount) from " + creditCardsData).first().getDouble(0)
  }

}
