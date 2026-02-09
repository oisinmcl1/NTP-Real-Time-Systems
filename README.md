# NTP Assignment

## Quick Start
```bash
./start.sh      # Start the container
./status.sh     # Check NTP status
./enter.sh      # Enter the container
./stop.sh       # Stop the container
```

## Container Details
- Container name: `ntp-assignment`
- User: `oisin_mclaughlin`
- Logs directory: `./ntp-logs`

## Useful Commands Inside Container
```bash
ntpq -p                  # Check NTP peers
service ntpsec status    # Check NTP service
su - oisin_mclaughlin    # Switch to your user
```

## Next Steps
1. Choose 6 specific NTP servers
2. Update ntp.conf
3. Rebuild with ./rebuild.sh
4. Set up cron job for data collection
5. Run for 8-12 hours
6. Analyze data in Excel