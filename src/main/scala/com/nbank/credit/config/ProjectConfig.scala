package com.nbank.credit.config

import com.typesafe.config.ConfigFactory

trait ProjectConfig {
  private val config=ConfigFactory.load()

  val data=config.getConfig("data")

    val creditCardFile=data.getString("creditCardFile.file")

}
