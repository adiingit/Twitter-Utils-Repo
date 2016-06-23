package com.qa.Util;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import twitter4j.Status;

public class SendTrendningMail implements Serializable{

	public static void sendMail(Map<Status, Long> mp) {

		final String username = "vprashant341@gmail.com";
		final String password = "Pra@1989";
		String messageBody = "";

		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress("vprashant341@gmail.com"));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse("prashantverma@qainfotech.com"));
			message.setSubject("Tredning people in automation world");
			for (Map.Entry<Status, Long> m : mp.entrySet()) {
				messageBody = messageBody.concat(
						"user : " + m.getKey().getUser().getName() 
						+" twittername : "+m.getKey().getUser().getScreenName()
						+" follower count : " + m.getValue() 
						+" status retweet count :"+m.getKey().getRetweetCount()
						+"\n"
						+"tweet : "+ m.getKey().getText()+"\n\n\n");
			}
			Transport.send(message);

			System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}

	}
}
