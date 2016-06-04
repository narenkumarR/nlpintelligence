import re
username = 'suriyah@contractiq.com'
password = 'suriyah54321'
# username = 'joswin.ideas2it@gmail.com'
# password = 'M!ttuN01an'
imap_server = 'imap.gmail.com'

# subjects = ['Scaling Engineering Teams','Scaling Mobile Initiatives for Brands','Mobile Report - ContractIQ',
# 	'Is your hiring plan on track?','Planning a conversation','Can we speak?','Include us in your 2015 planning',
# 	'Planning a meeting - 2nd week of Jan',"What's your plan for Tableau in 2015?",'Planning our conversation',
# 	'We should speak','Planning a conversation on digital innovation','Tech / M&A - Planning for a conversation',
# 	'Planning for a Conversation','Hello ! We are ContractIQ!','Conversation with you !',"Thank you. It's been a pleasure!",
# 	'A handy startup resource!','Tech & M&A Introductions','Game Development Teams - You might like this!',
# 	'Mobile aggregation - Can we speak?','Visiting California']

# subjects = ['Visiting California','New Employee Announcement','Donate','tips for using your new inbox','regarding documents']
# subjects = ['Quick question on the Makers Speak Report','Can we speak']
subjects = ['Scaling Engineering Teams']
subjects_matcher = re.compile('|'.join(subjects),re.IGNORECASE)