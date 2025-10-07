import base64
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from .config import settings

class EmailService:
    """
    Handles rendering and sending emails.
    It also handles embedding local images (header and footer) as Base64 data.
    """

    def __init__(self):
        self.mode = settings.EMAIL_MODE
        self.sender_email = settings.SENDER_EMAIL
        self.use_test_recipients = self.mode != 'SEND'
        self.test_recipients = settings.TEST_RECIPIENTS

        project_root = Path(__file__).parent.parent
        template_path = project_root / "templates"
        
        self.header_logo_path = project_root / "assets" / "headerlogo.png"
        self.footer_logo_path = project_root / "assets" / "footerlogo.png"
        
        self.jinja_env = Environment(loader=FileSystemLoader(template_path))

    def _encode_image_to_base64(self, image_path: Path) -> str | None:
        """Reads an image file and returns its Base64 encoded string."""
        try:
            with open(image_path, "rb") as image_file:
                encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            return encoded_string
        except FileNotFoundError:
            logging.warning(f"Logo image not found at path: {image_path}. Skipping embedding.")
            return None

    def _render_template(self, template_name: str, context: dict) -> str:
        """Renders an HTML template with the given context."""
        template = self.jinja_env.get_template(template_name)
        return template.render(context)

    def send_email(self, recipients: list, subject: str, template_name: str, context: dict):
        """
        Constructs and sends an email, ensuring recipient list is clean.
        """
        base_recipients = self.test_recipients if self.use_test_recipients else list(set(recipients))
        
        # --- MODIFICATION START: Clean the recipient list ---
        # This removes any empty strings, None values, or strings with only whitespace.
        final_recipients = [email for email in base_recipients if email and email.strip()]

        if not final_recipients:
            logging.warning(f"Recipient list for subject '{subject}' was empty after cleaning. Skipping email.")
            return
        # --- MODIFICATION END ---
        
        header_logo_data = self._encode_image_to_base64(self.header_logo_path)
        footer_logo_data = self._encode_image_to_base64(self.footer_logo_path)
        context['header_logo_base64'] = header_logo_data
        context['footer_logo_base64'] = footer_logo_data
        
        html_body = self._render_template(template_name, context)

        if self.mode == "LOG":
            logging.info("--- EMAIL DRY RUN (MODE=LOG) ---")
            logging.info(f"  From: {self.sender_email}")
            logging.info(f"  To: {', '.join(final_recipients)}")
            logging.info(f"  Subject: {subject}")
            logging.info("----------------------------------")
            return

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.sender_email
            msg["To"] = ", ".join(final_recipients)
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
                server.starttls()
                server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                server.sendmail(self.sender_email, final_recipients, msg.as_string())
            logging.info(f"Successfully sent email with subject '{subject}' to {', '.join(final_recipients)}")

        except Exception as e:
            logging.error(f"Failed to send email to {', '.join(final_recipients)}: {e}", exc_info=True)