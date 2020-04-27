// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test

import java.net.URL
import java.util.UUID

import com.daml.navigator.test.config.Arguments
import com.typesafe.scalalogging.LazyLogging
import org.openqa.selenium.{JavascriptExecutor, WebDriver}
import org.openqa.selenium.remote.DesiredCapabilities
import org.openqa.selenium.remote.RemoteWebDriver
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.selenium.WebBrowser
import org.scalatest.time.{Millis, Span}

import scala.sys.process._
import scala.util.{Failure, Success, Try}

@WrapWith(classOf[ConfigMapWrapperSuite])
class BrowserTest(args: Arguments)
    extends FlatSpec
    with Matchers
    with WebBrowser
    with BeforeAndAfterAll
    with BeforeAndAfter
    with Eventually
    with LazyLogging {

  // ------------------------------------------------------------------------------------------------------------------
  // Helpers for running Navigator and the sandbox
  // ------------------------------------------------------------------------------------------------------------------

  // The port the Navigator will be accessible on
  private val navigatorPort = args.navigatorPort
  // The port the sandbox will be accessible on
  private val sandboxPort = 8081

  // This id helps to avoid collision if multiple BS Locals are running on the same machine
  // See https://www.browserstack.com/local-testing#multiple-connections
  private val browserStackTestId = UUID.randomUUID().toString.replace("-", "")

  // These ids help finding the current session in the BrowserStack logs
  // See https://www.browserstack.com/automate/rest-api
  private val browserStackProjectName = "Navigator"
  private val browserStackBuildName = "NavigatorIT"
  private val browserStackSessionName = UUID.randomUUID().toString.replace("-", "")

  // Save a list of commands on how to destroy started processes
  private val componentsFixture =
    new ComponentsFixture(args, navigatorPort, sandboxPort, "Main:example")

  private val browserStackLocal = {
    val commands = List(
      "browserstack-local",
      "--key",
      args.browserStackKey,
      "--local-identifier",
      browserStackTestId)

    // Don't log the actual command, it contains credentials
    logger.info("Running BrowserStackLocal...")
    Process(commands).run
  }

  implicit private val webDriver: WebDriver = getBrowserStackDriver() match {
    case Success(d) => d
    case Failure(e) => fail(s"Can't run the test with BrowserSctack: ${e.getMessage}")
  }

  override def beforeAll(): Unit = {
    val started = for {
      _ <- componentsFixture.startup()
      _ <- componentsFixture.waitForNavigator()
    } yield ()
    started match {
      case Failure(e) =>
        logger.error(s"Error while starting up components: $e")
        throw e
      case Success(_) => ()
    }
  }

  override def afterAll(): Unit = {
    logger.info("Shutting down")
    Try(webDriver.quit())
    Try(componentsFixture.shutdown())
    Try(browserStackLocal.destroy())
    Try(BrowserStack.printLog(args, browserStackBuildName, browserStackSessionName))
    ()
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Definitions for identifying elements in the Navigator web page
  // ------------------------------------------------------------------------------------------------------------------

  private val singInPage = s"http://localhost:$navigatorPort/sign-in/"
  private val contractsPage = s"http://localhost:$navigatorPort/contracts/"
  private val templatesPage = s"http://localhost:$navigatorPort/templates/"

  // TODO use id's instead of xpath/classname to make it more robust (change Navigator code first)
  private val signOutButton = cssSelector("nav > div > div > button")
  private val choiceButton = cssSelector(
    "div.ReactVirtualized__Grid.ReactVirtualized__Table__Grid button > i")
  private val contractRow = cssSelector("div.ReactVirtualized__Table__row.ContractTable__row")
  private val contractsTable = cssSelector(
    "div.ReactVirtualized__Grid.ReactVirtualized__Table__Grid")
  private val archiveChoice = xpath("//li[span='Archive']")
  private val accept = xpath("//li[span='Accept']")
  private val contractDetails = xpath("//p[strong='Contract details']")
  private val landlord = xpath("//label[span='landlord']")
  private val contracts = xpath("//div[span='Contracts']")
  private val templates = xpath("//div[span='Templates']")
  private val timeButton = cssSelector("nav > div > div > div > button")
  private val setTimeButton = xpath("//button[span='Set']")
  private val specificDate = xpath("//tbody/tr[2]/td[2]")
  private val specificDateLabel = xpath("//button/span[contains(text(), '1970-01-05')]")
  private val rightOfUse = xpath("//div/div/div[starts-with(span,'Main:RightOfUse')]")
  private val rightOfUseAgreement = xpath(
    "//div/div/div[starts-with(span,'Main:RightOfUseAgreement')]")
  private val rightOfUseOffer = xpath("//div/div/div[starts-with(span,'Main:RightOfUseOffer')]")
  private val landlordInput = cssSelector("form > div > label:nth-child(1) > input")
  private val tenantInput = cssSelector("form > div > label:nth-child(2) > input")
  private val addressInput = cssSelector("form > div > label:nth-child(3) > input")
  private val expirationdateInput = cssSelector(
    "form > div > label:nth-child(4) > div > div > input")
  private val expirationdate = cssSelector("table > tbody > tr:nth-child(4) > td:nth-child(2)")
  private val submitContract = cssSelector("form > button")

  private def spanWith(text: String) = xpath(s"//span[contains(normalize-space(), '$text')]")

  // See navigator/frontend/src/ui-core/src/theme.ts
  private val tableRowHeight = 50

  private val user_bank1 = "Scrooge_McDuck"
  private val user_bank2 = "Betina_Beakley"
  private val user_operator = "Operator"

  // ------------------------------------------------------------------------------------------------------------------
  // Helpers for navigating the Navigator web page
  // ------------------------------------------------------------------------------------------------------------------

  private def goToSignIn(): Unit = {
    go to contractsPage

    // If we're not logged in, contracts page would redirect to sign-in page.
    eventually {
      if (currentUrl != singInPage) doClick(signOutButton)
      ()
    }

    eventually {
      currentUrl should be(singInPage)
      ()
    }
  }

  private def signIn(name: String): Unit = {
    goToSignIn()

    eventually {
      singleSel(tagName("select")).value = name
      ()
    }

    eventually {
      currentUrl should be(contractsPage)
      ()
    }
  }

  private def doClick(query: Query): Unit = {
    click on query
    ()
  }

  private def scrollToBottom(js: JavascriptExecutor, css: String): Unit = {
    js.executeScript(s"""
      var element = document.querySelector("$css");
      if (element) {
        console.log("ScrollToBottom: scrolling '$css' to " + element.scrollHeight)
        element.scrollTop = element.scrollHeight;
      } else {
        console.error("ScrollToBottom: could not find element '$css'")
      }
    """)
    ()
  }

  private def scrollDown(js: JavascriptExecutor, css: String, by: Int) = {
    js.executeScript(s"""
      var element = document.querySelector("$css");
      if (element) {
        var newTop = element.scrollTop + $by;
        // console.log("scrollDown: scrolling '$css' to " + newTop)
        element.scrollTop = newTop;
      } else {
        console.error("scrollDown: could not find element '$css'")
      }
    """)
  }

  // Check if a row with the given content is visible in a ReactVirtualized table
  private def isTableRowVisible(js: JavascriptExecutor, content: XPathQuery) = {
    js.executeScript(s"""
      // Table should always be rendered
      var table = document.querySelector("${contractsTable.queryString}");
      if (!table) {
        console.error("IsTableRowVisible: Could not find table '${contractsTable.queryString}'");
        return false;
      }

      // Row may not be rendered if it is far outside of the view
      var rows = document.evaluate("${content.queryString}", document);
      var row = rows.iterateNext()
      if (!row) {
        // console.log("IsTableRowVisible: Could not find row with content '${content.queryString}'");
        return false;
      }

      // Check if row is actually visible (it may be rendered outside of current view)
      var tableViewTop = table.scrollTop;
      var tableViewBottom = tableViewTop + table.clientHeight;
      var rowTop = row.offsetParent.offsetTop;
      var rowBottom = rowTop + row.offsetParent.clientHeight;
      // console.log(`table: [$${tableViewTop}, $${tableViewBottom}], row: [$${rowTop}, $${rowBottom}]`);
      return ((rowBottom >= tableViewTop) && (rowTop <= tableViewBottom));
    """)
  }

  // This config is used where we wait for the result in an "eventually" block
  // Note: Browser tests run on remote machines, we use a generous timeout.
  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10000, Millis)), interval = scaled(Span(100, Millis)))

  // Alternative config for "eventually" blocks, to be used after command submission.
  // Note: It may take a while to see updated contracts after a command submission.
  //   Currently, the Navigator frontend polls data every 5sec.
  private val commandSubmissionPatienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(15000, Millis)), interval = scaled(Span(100, Millis)))

  // ------------------------------------------------------------------------------------------------------------------
  // Tests that do not read contract data or submit commands. They can be run on an empty ledger, in any order.
  // ------------------------------------------------------------------------------------------------------------------

  "The user" should "be able to sign in and out" in {
    signIn(user_bank1)

    eventually { doClick(signOutButton) }
    eventually { currentUrl should be(singInPage) }
  }

  "Templates button" should "link to templates view" in {
    signIn(user_bank1)

    eventually { doClick(templates) }
    eventually { currentUrl should be(templatesPage) }
  }

  "Contracts button" should "link to contracts view" in {
    signIn(user_bank1)

    eventually { doClick(contracts) }
    eventually { currentUrl should be(contractsPage) }
  }

  "Time" should "be settable after sign in" in {
    signIn(user_bank1)

    eventually { doClick(timeButton) }

    eventually { doClick(specificDate) }

    eventually { doClick(setTimeButton) }

    eventually { find(specificDateLabel).isDefined should be(true) }
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Tests that read contracts or submit commands. The order of these tests is important.
  // ------------------------------------------------------------------------------------------------------------------

  "Contract" should "be created from a template" in {
    // Note: the ledger is empty initially
    signIn(user_bank2)

    // Open templates view
    eventually { doClick(templates) }
    eventually { currentUrl should be(templatesPage) }

    // Select the offer template - it might take a while to load the templates
    eventually { doClick(rightOfUseOffer) }
    eventually {
      currentUrl should startWith(s"http://localhost:$navigatorPort/templates/Main:RightOfUseOffer")
    }

    // Enter contract argument
    eventually {
      textField(landlordInput).value = user_bank2
      textField(tenantInput).value = user_bank1
      textField(addressInput).value = "McDuck Manor, Duckburg"
    }

    eventually { click on expirationdateInput }
    eventually { click on expirationdate }
    eventually { doClick(setTimeButton) }

    // Submit - we may need some time to close the date picker
    eventually { doClick(submitContract) }

    // Open templates view
    eventually { doClick(contracts) }
    eventually { currentUrl should be(contractsPage) }

    // There should be a single contract
    implicit def patienceConfig: PatienceConfig = commandSubmissionPatienceConfig
    eventually { findAll(rightOfUseAgreement).toList.size should be(1) }
  }

  "Choices" should "render properly" in {
    signIn(user_bank1)

    eventually { doClick(choiceButton) }

    eventually { find(archiveChoice).orElse(find(accept)).isDefined should be(true) }
  }

  "Contract data" should "be displayed" in {
    signIn(user_bank1)

    eventually { doClick(contractRow) }

    eventually { find(contractDetails).isDefined should be(true) }

    find(landlord).isDefined should be(true)
  }

  "Contracts" should "load on scroll" in {
    signIn(user_operator)

    doClick(templates)
    eventually { currentUrl should be(templatesPage) }

    // Open template contracts page
    eventually { doClick(spanWith("1000")) }
    eventually {
      currentUrl should startWith(s"http://localhost:$navigatorPort/templates/Main:Counter")
    }

    // Sort by index
    eventually { doClick(spanWith("index")) }

    // Wait until table is sorted by index
    eventually {
      find(spanWith("[001]")).isDefined should be(true)
      find(spanWith("[002]")).isDefined should be(true)
    }

    val js = webDriver.asInstanceOf[JavascriptExecutor]

    val lastOldElement = "[099]"
    val firstNewElement = "[100]"

    // Scroll to last row
    // Scrolling using absolute offsets is unreliable with ReactVirtualized,
    // we therefore scroll interactively down until the last element from the initial fetch is visible.
    var i = 0
    while (!isTableRowVisible(js, spanWith(lastOldElement)).asInstanceOf[Boolean]) {
      assert(i < 1000, s"Could not scroll to row '$lastOldElement' after 1000 tries")

      // Scroll down 1 row
      scrollDown(js, contractsTable.queryString, tableRowHeight)

      // Give ReactVirtualized time to re-render the table
      Thread.sleep(25)

      i = i + 1
    }

    // Scroll down some more - the first on-demand loaded contract would now be in view,
    // which definitely should trigger the corresponding fetch.
    scrollDown(js, contractsTable.queryString, 2 * tableRowHeight)

    // Check the presence of the first on-demand loaded contract
    eventually { find(spanWith(firstNewElement)).isDefined should be(true) }
  }

  // ------------------------------------------------------------------------------------------------------------------
  // Other helpers.
  // ------------------------------------------------------------------------------------------------------------------

  private def getBrowserStackDriver(): Try[WebDriver] = {
    val userName = args.browserStackUser
    val automateKey = args.browserStackKey
    val url = s"https://$userName:$automateKey@hub-cloud.browserstack.com/wd/hub"

    val caps = new DesiredCapabilities
    // Note that if testing Safari with BrowserStack, you can use only a subset of ports:
    // https://www.browserstack.com/question/664
    caps.setCapability("browser", "Chrome")
    caps.setCapability("browser_version", "66.0")
    caps.setCapability("os", "OS X")
    caps.setCapability("os_version", "High Sierra")
    caps.setCapability("resolution", "1024x768")

    // Test identification
    caps.setCapability("project", browserStackProjectName)
    caps.setCapability("build", browserStackBuildName)
    caps.setCapability("name", browserStackSessionName)

    // Enable BrowserStack debugging info
    caps.setCapability("browserstack.console", "info")
    caps.setCapability("browserstack.appiumLogs", "true")
    caps.setCapability("browserstack.networkLogs", "false")
    caps.setCapability("browserstack.debug", "false")

    // Enable BrowserStack local mode
    caps.setCapability("browserstack.local", "true")
    caps.setCapability("browserstack.localIdentifier", browserStackTestId)

    retry(new RemoteWebDriver(new URL(url), caps), 120, 1000)
  }

  private def retry[R](action: => R, maxRetries: Int, delayMillis: Int): Try[R] = {
    def retry0(count: Int): Try[R] = {
      Try(action) match {
        case Success(r) => Success(r)
        case Failure(e) =>
          if (count > maxRetries) {
            Failure(e)
          } else {
            Thread.sleep(delayMillis.toLong)
            retry0(count + 1)
          }
      }
    }

    retry0(0)
  }
}
