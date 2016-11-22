# Sitecore.Support.442487
When using the OnPublishEndAsynchronous strategy with CheckForThreshold set to true, indexes may be fully rebuilt after every content item update.

The following multiple messages may be logged when the issue occurs:
```
WARN  [Index=sitecore_web_index] The number of changes exceeded maximum threshold of '100000'.
WARN  IndexCustodian. FullRebuild triggered on index sitecore_web_index
```

## License  
This patch is licensed under the [Sitecore Corporation A/S License for GitHub](https://github.com/sitecoresupport/Sitecore.Support.442487/blob/master/LICENSE).  

## Download  
Downloads are available via [GitHub Releases](https://github.com/sitecoresupport/Sitecore.Support.442487/releases).  
