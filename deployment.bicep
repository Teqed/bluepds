param webAppName string
param location string = resourceGroup().location // Location for all resources

param sku string = 'B1' // The SKU of App Service Plan
param dockerContainerName string = '${webAppName}:latest'
param repositoryUrl string = 'https://github.com/DrChat/bluepds'
param branch string = 'main'
param customDomain string

@description('Redeploy hostnames without SSL binding. Just specify `true` if this is the first time you\'re deploying the app.')
param redeployHostnamesHack bool = false

var acrName = toLower('${webAppName}${uniqueString(resourceGroup().id)}')
var aspName = toLower('${webAppName}-asp')
var webName = toLower('${webAppName}${uniqueString(resourceGroup().id)}')
var sanName = toLower('${webAppName}${uniqueString(resourceGroup().id)}')

// resource appInsights 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
//   name: '${webAppName}-ai'
//   location: location
//   properties: {
//     publicNetworkAccessForIngestion: 'Enabled'
//     workspaceCapping: {
//       dailyQuotaGb: 1
//     }
//     sku: {
//       name: 'Standalone'
//     }
//   }
// }

// resource appServicePlanDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
//   name: appServicePlan.name
//   scope: appServicePlan
//   properties: {
//     workspaceId: appInsights.id
//     metrics: [
//       {
//         category: 'AllMetrics'
//         enabled: true
//       }
//     ]
//   }
// }

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

resource appStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: sanName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
}

resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-05-01' = {
  name: '${appStorage.name}/default/data'
  properties: {}
}

resource appService 'Microsoft.Web/sites@2020-06-01' = {
  name: webName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    httpsOnly: true
    serverFarmId: appServicePlan.id
    siteConfig: {
      // Sigh. This took _far_ too long to figure out.
      // We must authenticate to ACR, as no credentials are set up by default
      // (the Az CLI will implicitly set them up in the background)
      acrUseManagedIdentityCreds: true
      appSettings: [
        {
          name: 'BLUEPDS_HOST_NAME'
          value: empty(customDomain) ? '${webName}.azurewebsites.net' : customDomain
        }
        {
          name: 'BLUEPDS_TEST'
          value: 'false'
        }
        {
          name: 'WEBSITES_PORT'
          value: '8000'
        }
      ]
      linuxFxVersion: 'DOCKER|${acrName}.azurecr.io/${dockerContainerName}'
    }
  }
}

resource hostNameBinding 'Microsoft.Web/sites/hostNameBindings@2024-04-01' = if (redeployHostnamesHack) {
  name: customDomain
  parent: appService
  properties: {
    siteName: appService.name
    hostNameType: 'Verified'
    sslState: 'Disabled'
  }
}

// This stupidity is required because Azure requires a circular dependency in order to define a custom hostname with SSL.
// https://stackoverflow.com/questions/73077972/how-to-deploy-app-service-with-managed-ssl-certificate-using-arm
module certificateBindings './deploymentBindingHack.bicep' = {
  name: '${deployment().name}-ssl'
  params: {
    appServicePlanResourceId: appServicePlan.id
    customHostnames: [customDomain]
    location: location
    webAppName: appService.name
  }
  dependsOn: [hostNameBinding]
}

resource appServiceStorageConfig 'Microsoft.Web/sites/config@2024-04-01' = {
  name: 'azurestorageaccounts'
  parent: appService
  properties: {
    data: {
      type: 'AzureFiles'
      shareName: 'data'
      mountPath: '/app/data'
      accountName: appStorage.name
      // WTF? Where's the ability to mount storage via managed identity?
      accessKey: appStorage.listKeys().keys[0].value
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
