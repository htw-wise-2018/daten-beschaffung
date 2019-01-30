package observer

import java.io.IOException
import org.apache.commons.net.ftp
import org.apache.commons.net.ftp.FTPClient
import akka.actor.Actor
import akka.actor.Timers
import akka.actor.Props
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future
import akka.actor.ActorLogging

object FtpObserver {
  def props(callback: ()=>Unit) = Props(new FtpObserver(callback))
}
case object Start
case object TimerKey
case object Polling
case object Stop
class FtpObserver (
  val callback: () => Unit,
  val server:   String = "ftp.ifremer.fr",
  val port:     Int    = 21,
  val user:     String = "anonymous",
  val password: String = "password") extends Actor with ActorLogging with Timers{

  timers.startSingleTimer(TimerKey, Start, 0 second)
  
  override def receive: Receive = {
    case Start => {
      log.info("Start ftp-polling")
      self ! Polling // First poll
      timers.startPeriodicTimer(TimerKey, Polling, 1 day) // Periodic polling
    }
    case Polling => {
      log.info("Poll")
      val currentModificationTime = getModificationTime()
      log.info(s"Current modification time: ${currentModificationTime}")
        if (lastModificationTime != currentModificationTime) {
          lastModificationTime = currentModificationTime
          callback()
        }
    }
    case Stop => {
      log.warning("Stopping")
      timers.cancel(TimerKey)
      context.stop(self)
    }
      
  }
  
  var lastModificationTime = ""
  
  @throws(classOf[Exception])
  def getModificationTime(filePath: String = "/ifremer/argo/ar_index_this_week_prof.txt"): String = {
    val ftp = new FTPClient()
    ftp.connect(server, port)
    ftp.login(user, password)
    val currentModificationTime = ftp.getModificationTime(filePath)
    ftp.disconnect()
    currentModificationTime

  }

}
