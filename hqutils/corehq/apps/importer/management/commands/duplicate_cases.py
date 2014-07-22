from datetime import datetime
import os
import re
from django.core.management import BaseCommand, CommandError
from casexml.apps.case.models import CommCareCase
from corehq.apps.receiverwrapper.util import get_submit_url
from corehq.apps.users.models import CouchUser
from couchforms.models import XFormInstance
from dimagi.utils.post import simple_post

CONF = 'duplicate_cases.conf'

URL_BASE = 'https://www.commcarehq.org'

class Command(BaseCommand):
    help = "import cases from excel manually."
    args = '<case_id file> <user> <to domain>'
    label = "import cases from excel manually."

    processed_docs = []
    cases_processing = {}
    forms_processing = {}

    def handle(self, *args, **options):
        if len(args) != 3:
            raise CommandError('Usage is duplicate_cases %s' % self.args)

        start = datetime.now()
        case_id_file, user_id, domain = args
        self.submit_url = URL_BASE + get_submit_url(domain)
        print self.submit_url

        user = CouchUser.get_by_user_id(user_id, domain)
        user_id = user._id
        if not user.is_member_of(domain):
            raise CommandError("%s can't access %s" % (user, domain))

        self.read_progress()

        with open(case_id_file, 'r') as f:
            case_ids = f.readlines()

        try:
            for id in case_ids:
                self.duplicate_case(id.strip(), domain, user_id)
        finally:
            self.write_progress()

        print 'finished in %s seconds' % (datetime.now() - start).seconds
        print '{} cases processed'.format(len(self.cases_processing.keys()))
        print '{} forms processed'.format(len(self.forms_processing.keys()))

    def new_id(self):
        return XFormInstance.get_db().server.next_uuid()

    def duplicate_case(self, case_id, new_domain, new_owner, level=0):
        if case_id not in self.cases_processing:
            self.cases_processing[case_id] = self.new_id()
        else:
            return

        print '{}--- case: {} -> {}'.format('    ' * level, case_id, self.cases_processing[case_id])

        case = CommCareCase.get(case_id)
        xforms = case.xform_ids
        referenced_cases = [i.referenced_id for i in case.indices]
        for c in referenced_cases:
            self.duplicate_case(c, new_domain, new_owner, level=level+1)

        for xform in xforms:
            self.process_xform(xform, new_domain, new_owner, level)

        self.processed_docs.append(('case', case_id, self.cases_processing[case_id]))

    def process_xform(self, form_id, new_domain, new_owner, level):
        if form_id not in self.forms_processing:
            self.forms_processing[form_id] = self.new_id()
        else:
            return

        print "{}=== xform: {} -> {}".format('    ' * (level+1), form_id, self.forms_processing[form_id])

        instance = XFormInstance.get(form_id)
        xml = instance.get_xml()
        referenced_case_ids = set(re.findall(r'case_id="([\w-]*)"', xml))
        for ref_case_id in referenced_case_ids:
            if ref_case_id not in self.cases_processing:
                self.duplicate_case(ref_case_id, new_domain, new_owner, level=level+1)

            xml = xml.replace(ref_case_id, self.cases_processing[ref_case_id])

        referenced_user_ids = set(re.findall(r'user_id="([\w-]*)"', xml))
        for ref_form_id in referenced_user_ids:
            xml = xml.replace(ref_form_id, new_owner)

        instance_ids = set(re.findall(r'instanceID>([\w-]*)</', xml))
        for inst_id in instance_ids:
            xml = xml.replace(inst_id, self.forms_processing[form_id])

        resp = simple_post(xml, self.submit_url, content_type='text/xml')
        if not resp.status in [200, 201]:
            raise Exception(resp.read())
        # with open("{}/{}.xml".format(OUT, self.forms_processing[form_id]), "w") as form:
        #     form.write(xml)

        self.processed_docs.append(('form', form_id, self.forms_processing[form_id]))

    def write_progress(self):
        lines = ['doc_type,old_id,new_id\n']
        lines.extend(['{}\n'.format(','.join(row)) for row in self.processed_docs])
        with open('duplicate_cases.conf', 'w') as f:
            f.writelines(lines)

    def read_progress(self):
        if not os.path.isfile(CONF):
            return

        with open(CONF, 'r') as f:
            lines = f.readlines()

        for line in lines[1:]:
            doc_type, old, new = line.strip().split(',')
            self.processed_docs.append((doc_type, old, new))
            if doc_type == 'case':
                self.cases_processing[old] = new
            elif doc_type == 'form':
                self.forms_processing[old] = new
