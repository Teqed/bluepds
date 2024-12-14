param webAppName string
param location string = resourceGroup().location // Location for all resources

param sku string = 'F1' // The SKU of App Service Plan
param dockerContainerName string = '${webAppName}:latest'
param repositoryUrl string = 'https://github.com/DrChat/azure-rust-app'
param branch string = 'main'

var acrName = toLower('${webAppName}${uniqueString(resourceGroup().id)}')
var aspName = toLower('${webAppName}-asp')
var webName = toLower('${webAppName}${uniqueString(resourceGroup().id)}')

resource appServicePlan 'Microsoft.Web/serverfarms@2020-06-01' = {
  name: aspName
  location: location
  properties: {
    reserved: true
  }
  sku: {
    name: sku
  }
  kind: 'linux'
}

resource acrResource 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' = {
  name: acrName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
  }
}

resource appService 'Microsoft.Web/sites@2020-06-01' = {
  name: webName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      // Sigh. This took _far_ too long to figure out.
      // We must authenticate to ACR, as no credentials are set up by default
      // (the Az CLI will implicitly set them up in the background)
      acrUseManagedIdentityCreds: true
      appSettings: [
        {
          name: 'WEBSITES_PORT'
          value: '8000'
        }
      ]
      linuxFxVersion: 'DOCKER|${acrName}.azurecr.io/${dockerContainerName}'
    }
  }
}

@description('This is the built-in AcrPull role. See https://docs.microsoft.com/azure/role-based-access-control/built-in-roles#acrpull')
resource acrPullRoleDefinition 'Microsoft.Authorization/roleDefinitions@2018-01-01-preview' existing = {
  scope: subscription()
  name: '7f951dda-4ed3-4680-a7ca-43fe172d538d'
}

resource appServiceAcrPull 'Microsoft.Authorization/roleAssignments@2020-04-01-preview' = {
  name: guid(resourceGroup().id, acrResource.id, appService.id, 'AssignAcrPullToAS')
  scope: acrResource
  properties: {
    description: 'Assign AcrPull role to AS'
    principalId: appService.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRoleDefinition.id
  }
}

resource srcControls 'Microsoft.Web/sites/sourcecontrols@2021-01-01' = {
  name: 'web'
  parent: appService
  properties: {
    repoUrl: repositoryUrl
    branch: branch
    isManualIntegration: true
  }
}

output acr string = acrResource.name
output domain string = appService.properties.hostNames[0]
