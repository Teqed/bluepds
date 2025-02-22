// https://stackoverflow.com/questions/73077972/how-to-deploy-app-service-with-managed-ssl-certificate-using-arm
//
// TLDR: Azure requires a circular dependency in order to define an app service with a custom domain with SSL enabled.
// Terrific user experience. Really makes me love using Azure in my free time.
param webAppName string
param location string
param appServicePlanResourceId string
param customHostnames array

// Managed certificates can only be created once the hostname is added to the web app.
resource certificates 'Microsoft.Web/certificates@2022-03-01' = [for (fqdn, i) in customHostnames: {
  name: '${fqdn}-${webAppName}'
  location: location
  properties: {
    serverFarmId: appServicePlanResourceId
    canonicalName: fqdn
  }
}]

// sslState and thumbprint can only be set once the managed certificate is created
@batchSize(1)
resource customHostname 'Microsoft.web/sites/hostnameBindings@2019-08-01' = [for (fqdn, i) in customHostnames: {
  name: '${webAppName}/${fqdn}'
  properties: {
    siteName: webAppName
    hostNameType: 'Verified'
    sslState: 'SniEnabled'
    thumbprint: certificates[i].properties.thumbprint
  }
}]
