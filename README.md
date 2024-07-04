# Automotive Demos

This project contains sample code for the [Data analytics for automotive test fleets](https://learn.microsoft.com/en-us/azure/architecture/industries/automotive/automotive-telemetry-analytics) architecture.

Here you can find two sub-projects:

- [fabric](fabric/README.md) shows how to analyze MDF files in Microsoft Fabric.
- [mdf42adx](mdf42adx/README.md) shows how to analyze MDF files in ADX.

The [ASAM MDF-4 standard](https://www.asam.net/standards/detail/mdf/wiki/) has wide adoption in the automotive industry to store measurement and calibration data. The [asammdf python library](https://pypi.org/project/asammdf/) provides structured access to the MDF-4 data.

## Using the projects

A easy way to get started is to use Visual Studio Code and WSL.

- Install WSL2 in your Windows computer
- Install Visual Studio Code
- Check out the github repository
- Open Visual Studio Code using the desired directory as argument, for example

``` bash
code mdf42adx
code fabric
```

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
