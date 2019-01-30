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

/** Companion Object as recommended practice for Akka actors 
  * 
  * props is a factory method to construct the ftpobserver object
  * 
  * @author Raimi Solorzano Niederhausen - s0557978@htw-berlin.de Contact
  * @see See [[https://doc.akka.io/docs/akka/2.5.5/scala/actors.html#recommended-practices]] for more
  * information on Akka's recommended practices for actors 
  */
object FtpObserver {
  def props(callback: ()=>Unit) = Props(new FtpObserver(callback))
}
case object Start
case object TimerKey
case object Polling
case object Stop
/** Class to check for modifications at the ftp server permanently in request intervals.
  *
  * @constructor Creates a ftp observer object.
  * @param callback Is a function that is called by the actor when relevant modifications were detected at the ftp 
  * @param server Ftp server to observe
  * @param port Ftp port (default ftp port 21)
  * @param user Ftp user (no user necessary)
  * @param password Ftp password (no password necessary)
  * @author Raimi Solorzano Niederhausen - s0557978@htw-berlin.de Contact
  * @see See [[https://github.com/htw-wise-2018]] for more information.
  */
class FtpObserver (
  val callback: () => Unit,
  val server:   String = "ftp.ifremer.fr",
  val port:     Int    = 21,
  val user:     String = "anonymous",
  val password: String = "password") extends Actor with ActorLogging with Timers{

  /** Akka timer to start the observation task 
    * TimerKey serves as unique id for the timer
    * Start is the signal to send to initialize the Akka actor
    * 0 second is the initial delay
    */
  timers.startSingleTimer(TimerKey, Start, 0 second)
  
  // Method to receive messages for the actor
  override def receive: Receive = {
    case Start => {
      log.info("Start ftp-polling")
      self ! Polling // Actor sends message to itself. Start first poll.
      timers.startPeriodicTimer(TimerKey, Polling, 1 day) // Periodic polling. Sends 'Polling' message every day to the actor
    }
    case Polling => { // Check modification of specific file at ftp server
      log.info("Poll")
      val currentModificationTime = getModificationTime()
      log.info(s"Current modification time: ${currentModificationTime}")
        if (lastModificationTime != currentModificationTime) { // if file modified
          lastModificationTime = currentModificationTime
          callback()
        }
    }
    case Stop => { // Stop actor 
      log.warning("Stopping")
      timers.cancel(TimerKey)
      context.stop(self)
    }
      
  }
  
  var lastModificationTime = "" // saves last modification time to compare with the new modification time
  
  /** Check modification of specific file at ftp server
    * @param filePath Path of file to observe. Path relative to defined ftp server.
    */
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
