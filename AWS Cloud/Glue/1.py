s = 'abcabc'
print("length of string : ",len(s))
#for the number of characters in n
for idx in range(1, len(s)):
    #if idx fits evenly in to the length of s
    if len(s) % idx == 0:
        print(idx)
        #remember the first sliding window value
        subst = s[0 : idx]
        print(subst)
        for part in range(1, len(s) // idx):
            #if current doesn't match first sliding window value
            print("win---dow :",part)
##################################################################





Rule_Hourly_Mon-Fri

cron(0 9-22 ? * MON-FRI *)

cron(30 3-16 ? * MON-FRI *) -- base tables

cron(45 3-16 ? * MON-FRI *) -- for Derived tables
