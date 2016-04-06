__author__ = 'joswin'

company_stops_regex = 'inc|limited|ltd|technologies|tecnology|services|service|llc'
mail_end_regex = r'\.co|\.org|\.gov'

confidences = {'profile page with company match':95,'profile page with previous company match':85,
               'profile page with company match on page text':80,
               'profile page with company match on page text (also viewed part)':80,
               'profile page without company match':50,
               'company page':50}