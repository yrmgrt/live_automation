from django.db.models.options import Options
import re

regex = re.compile(r"[A-Z][^A-Z]*")

previous = Options.contribute_to_class


def contribute_to_class(self, *args, **kwargs):
    previous(self, *args, **kwargs)
    name = self.object_name
    old = self.db_table
    if '_' not in name and 'django' not in old:
        words = regex.findall(name)
        word = '_'.join(words).lower()
        self.db_table = "%s_%s" % (self.app_label, word)
        self.model_name_processed = "%s" % (word,)


Options.contribute_to_class = contribute_to_class
