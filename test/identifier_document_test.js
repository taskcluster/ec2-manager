const {validate} = require('../lib/identity-document');
const assume = require('assume');

const testData = {
  signedData: `
MIAGCSqGSIb3DQEHAqCAMIACAQExCzAJBgUrDgMCGgUAMIAGCSqGSIb3DQEHAaCAJIAEggHVewog
ICJkZXZwYXlQcm9kdWN0Q29kZXMiIDogbnVsbCwKICAibWFya2V0cGxhY2VQcm9kdWN0Q29kZXMi
IDogbnVsbCwKICAiYXZhaWxhYmlsaXR5Wm9uZSIgOiAidXMtZWFzdC0xZCIsCiAgInZlcnNpb24i
IDogIjIwMTctMDktMzAiLAogICJpbnN0YW5jZUlkIiA6ICJpLTAzYWUxNGMzNDZiYTY0M2ExIiwK
ICAiYmlsbGluZ1Byb2R1Y3RzIiA6IG51bGwsCiAgImluc3RhbmNlVHlwZSIgOiAibTQueGxhcmdl
IiwKICAiYXJjaGl0ZWN0dXJlIiA6ICJ4ODZfNjQiLAogICJrZXJuZWxJZCIgOiBudWxsLAogICJy
YW1kaXNrSWQiIDogbnVsbCwKICAiYWNjb3VudElkIiA6ICI2OTI0MDYxODM1MjEiLAogICJpbWFn
ZUlkIiA6ICJhbWktNDI3YjRlMzgiLAogICJwZW5kaW5nVGltZSIgOiAiMjAxOC0wMS0yMlQxMjoy
OTo1NFoiLAogICJwcml2YXRlSXAiIDogIjE3Mi4zMS4yMy4yMjUiLAogICJyZWdpb24iIDogInVz
LWVhc3QtMSIKfQAAAAAAADGCARgwggEUAgEBMGkwXDELMAkGA1UEBhMCVVMxGTAXBgNVBAgTEFdh
c2hpbmd0b24gU3RhdGUxEDAOBgNVBAcTB1NlYXR0bGUxIDAeBgNVBAoTF0FtYXpvbiBXZWIgU2Vy
dmljZXMgTExDAgkAlrpI2eVeGmcwCQYFKw4DAhoFAKBdMBgGCSqGSIb3DQEJAzELBgkqhkiG9w0B
BwEwHAYJKoZIhvcNAQkFMQ8XDTE4MDEyMjEyMzAwMlowIwYJKoZIhvcNAQkEMRYEFK1hHB7W5la2
AWAHCWVgYPYyJzAxMAkGByqGSM44BAMELzAtAhUAsQXD04cP48o7HVHWJtVRHZEUkBICFHcuPVAu
7KVSbiWnFnDL0v87RSxhAAAAAAAA`,
  doc:
`{
  "devpayProductCodes" : null,
  "marketplaceProductCodes" : null,
  "availabilityZone" : "us-east-1d",
  "version" : "2017-09-30",
  "instanceId" : "i-03ae14c346ba643a1",
  "billingProducts" : null,
  "instanceType" : "m4.xlarge",
  "architecture" : "x86_64",
  "kernelId" : null,
  "ramdiskId" : null,
  "accountId" : "692406183521",
  "imageId" : "ami-427b4e38",
  "pendingTime" : "2018-01-22T12:29:54Z",
  "privateIp" : "172.31.23.225",
  "region" : "us-east-1"
}`,
  invalidDoc:
`{
  "devpayProductCodes" : null,
  "marketplaceProductCodes" : null,
  "availabilityZone" : "us-east-1d",
  "version" : "2017-09-30",
  "instanceId" : "i-03ae14c346ba643a1",
  "billingProducts" : null,
  "instanceType" : "m4.xlarge",
  "architecture" : "x86_64",
  "kernelId" : null,
  "ramdiskId" : null,
  "accountId" : "692406183521",
  "imageId" : "ami-427b4e38",
  "pendingTime" : "2018-01-22T12:29:54Z",
  "privateIp" : "172.31.23.225",
  "region" : "us-west-1"
}`,
};

describe('Instance identifier document', () => {
  it('valid', () => {
    assume(validate(testData.doc, testData.signedData)).to.be.true();
  });

  it('invalid document', () => {
    assume(validate(testData.invalidDoc, testData.signedData)).to.be.false();
  });
});
