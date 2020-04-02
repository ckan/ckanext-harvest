
import logging

from pylons import config, app_globals

from ckan import logic
from ckan.logic import get_action

from ckan.plugins import toolkit


from ckanext.harvest.logic.dictization import harvest_job_dictize

import ckan.lib.mailer as mailer

log = logging.getLogger(__name__)



def send_error_mail_ncar(context, job_obj):

    sql = 'select name from package where id = :source_id;'

    model = context['model']

    q = model.Session.execute(sql, {'source_id': job_obj.source.id})

    for row in q:
        harvest_name = str(row['name'])

    ckan_site_url = config.get('ckan.site_url')
    job_url = ckan_site_url + '/harvest/' + harvest_name + '/job/' + job_obj.id

    msg = 'This is a failure-notification of the latest harvest job on ' + ckan_site_url + '.\n\n'
    msg += 'Harvest Job URL: ' + job_url + '\n\n'

    sql = '''select g.title as org, s.title as job_title from member m
            join public.group g on m.group_id = g.id
            join harvest_source s on s.id = m.table_id
            where table_id = :source_id;'''

    q = model.Session.execute(sql, {'source_id': job_obj.source.id})

    for row in q:
        orgName = str(row['org'])
        msg += 'Organization: ' + str(row['org']) + '\n\n'
        msg += 'Harvest Source: ' + str(row['job_title']) + '\n\n'

    msg += 'Date of Harvest: ' + str(job_obj.created) + ' GMT\n\n'

    out = {
        'last_job': None,
    }

    out['last_job'] = harvest_job_dictize(job_obj, context)

    job_dict = get_action('harvest_job_report')(context, {'id': job_obj.id})
    error_dicts = job_dict['object_errors']
    errored_object_keys = error_dicts.keys()
    numRecordsInError = len(errored_object_keys)
    msg += 'Records in Error: ' + str(numRecordsInError) + '\n\n'

    msg += 'For help, please contact the DASH Data Curation and Stewardship Coordinator (mailto:datahelp@ucar.edu).\n\n\n'

    if numRecordsInError <= 20:
       errored_object_keys = errored_object_keys[:20]
       for key in errored_object_keys:
           error_dict = error_dicts[key]
           msg += error_dict['original_url'] + ' :\n\n'
           for error in error_dict['errors']:
               msg += error['message']
               if error['line']:
                  msg += ' (line ' + str(error['line']) + ')\n\n'
               else:
                  msg += '\n'
           msg += '\n\n'
    else:
       for key in errored_object_keys:
           msg += error_dicts[key]['original_url'] + '\n\n'
       msg += '\n\nError Messages are suppressed if there are more than 20 records with errors.\n'

    log.debug("msg == " + msg)

    if numRecordsInError > 0:
        msg += '\n--\nYou are receiving this email because you are currently set-up as a member of the Organization "' + orgName + '" for ' + config.get('ckan.site_title') + '. Please do not reply to this email as it was sent from a non-monitored address.'

        # get org info
        log.debug('orgName == ' + orgName)
        org_dict = toolkit.get_action('organization_show')(context, {'id' : orgName.lower(), 'include_users': True})

        # get usernames in org
        usernames = [x['name'] for x in org_dict['users']]
        log.debug("usernames == " + ','.join(usernames))

        # get emails for users 
        email_recipients = []
        for username in usernames:
            user_dict = toolkit.get_action('user_show')(context, {'id' : username})
            email_recipients.append(user_dict['email'])

        log.debug("email_recipients == " + ','.join(email_recipients))
        emails = {}

        for recipient in email_recipients:
            email = {'recipient_name': recipient,
                      'recipient_email': recipient,
                      'subject': config.get('ckan.site_title') + ' - Harvesting Job - Error Notification',
                      'body': msg}

            try:
                app_globals._push_object(config['pylons.app_globals'])
                mailer.mail_recipient(**email)
            except Exception as e:
                log.exception(e)
                log.error('Sending Harvest-Notification-Mail failed. Message: ' + msg)


