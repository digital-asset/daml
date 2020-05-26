# Example scripts

This directory contains some helper scripts. These assume a physical machine with a 
"builder" account configured.

## com.digitalasset.macos-builder-crontab.plist

Example script for Launchctl scheduled rebuild of MacOS nodes. Run via

```bash
launctl load com.digitalasset.macos-builder-crontab.plist
```

and unload via 

```bash
launctl unload com.digitalasset.macos-builder-crontab.plist
```

NOTE: AbandonProcessGroup key is requied for VMWare or the started vmx process is sent a SIGTERM signal by launchd,
causing node to halt and not be left running at end of script. Different behaviour to VirtualBox.

## rebuild-crontask.sh

Script called by launchctl job to force destroy and rebuild a VSTS node. Needs a PAT token with job access 
rights to check no active jobs on node being rebuilt. 

## run-agent.sh

Helper script to build a node. Called from the above script. Needs a PAT token for agent installation. 


# Other Catalina Topics

* Disable Energy Saver settings in System Preferences to stop devices going into sleep mode
* Delete GarageBand, iMovie, KeyNote, Pages, Sheets to save space and avoid annoying Update notification
* May need to download Catalina installer on new release to ensure latest patched version
* Consider creating SSH key pair and distribute public key to other nodes in authorized_keys to distrbute 
files more easily 
