#!/bin/sh
#
# Recreate "all.pem" from Firefox certificates and Certifi Python lib.
# 
# More info:
#   About Firefox https://github.com/curl/curl/blob/master/lib/firefox-db2pem.sh
#   About Certifi https://certifi.io/en/latest/
#


# Go to script path
SCRIPT="$(readlink --canonicalize-existing "$0")"
SCRIPTPATH="$(dirname "$SCRIPT")"
cd $SCRIPTPATH


# ===============================================================================
# Certificates from Firefox
# ===============================================================================
echo "Building 'ca-firefox.crt' from Firefox..."

db=`ls -1d $HOME/.mozilla/firefox/*default*`
out=$1

if test -z "$out"; then
  out="partials/ca-firefox.crt" # use a sensible default
  rm -f $out
fi

currentdate=`date`

cat >$out <<EOF
##
## Bundle of CA Root Certificates
##
## Converted at: ${currentdate}
## These were converted from the local Firefox directory by the db2pem script.
##
EOF

certutil -L -h 'Builtin Object Token' -d $db | \
grep ' *[CcGTPpu]*,[CcGTPpu]*,[CcGTPpu]* *$' | \
sed -e 's/ *[CcGTPpu]*,[CcGTPpu]*,[CcGTPpu]* *$//' -e 's/\(.*\)/"\1"/' | \
sort | \
while read nickname; \
 do echo $nickname | sed -e "s/Builtin Object Token://g"; \
eval certutil -d $db -L -n "$nickname" -a ; \
done >> $out



# ===============================================================================
# Certificates from Certifi
# ===============================================================================
echo "Building 'ca-certifi.crt' from mkcert.org (Certifi Python lib)..."
curl -s https://mkcert.org/generate/ > partials/ca-certifi.crt



# ===============================================================================
# Concatenate and create all.pem
# ===============================================================================
echo "Building 'all.pem'..."
rm -f all.pem
cat partials/* > all.pem


echo "Done!"