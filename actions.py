import smtplib

import settings

class Action(object):
    def __init__(self, **settings):
        self.settings = settings

    def alert(self):
        pass # Override me

    @classmethod
    def get_actions(cls):
        acts = []
        for subcls in cls.__subclasses__():
            acts.append({
                'name': subcls.get_name(),
                'description': subcls.__doc__,
            })
        return acts

    @classmethod
    def get_name(cls):
        return getattr(cls, 'name', cls.__name__)

class EMail(Action):
    """Sends e-mail alerts"""
    name = 'E-Mail'
    def alert(self):
        s = smtplib.SMTP(settings.SMTP_HOST)
        body = """From: %s
To: %s
Subject: %s

ALERT!""" % (settings.MAIL_FROM, settings.MAIL_TO, 'Test alert')
        s.sendmail(settings.MAIL_FROM, [settings.MAIL_TO], body)
        s.quit()
