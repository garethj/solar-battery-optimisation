# Solar Battery Optimisation

An AWS Lambda that squeezes more value from a home battery by coordinating cheap-rate charging, solar forecasts, and grid export timing.

It controls a GivEnergy battery on Octopus Go, making four decisions throughout the day:

1. **Charge overnight** (23:30-05:30) when electricity is cheap
2. **Export before solar peaks** to empty the battery just as panels start clipping, so it can absorb excess generation
3. **Export again after solar** to sell solar-recharged energy at peak rates before off-peak starts
4. **Pause if the EV is plugged in**, since discharge would flow into the car instead of the grid

## How it works

### Tariff arbitrage

Octopus Go has a cheap off-peak window (23:30-05:30). The script charges the battery during off-peak, then exports during peak hours at the higher rate.

### Maximising solar capture

The inverter caps at 3.68 kW total output. On sunny days, panels can generate more than this, but excess is clipped and wasted unless the battery has room to absorb it.

The script empties the battery before solar peaks:

1. Checks the Solcast forecast for when generation will approach the inverter limit (~3.50 kW, with a 5% variance buffer)
2. Calculates discharge time at 2.6 kW, accounting for rising solar generation reducing the effective discharge rate (solar and battery discharge share the 3.68 kW inverter capacity)
3. Times the export to finish just as solar hits its peak
4. The empty battery then absorbs excess solar that would otherwise be clipped

After solar peak, the battery recharges with free solar energy, ready for another export.

### Consumption protection

Before exporting, the script reserves enough battery for predicted household consumption until off-peak. It estimates this from yesterday's usage (with a 10% buffer) minus the pessimistic solar forecast. This avoids expensive peak grid purchases from over-exporting.

### EV override

If the EV is plugged in, exporting pauses. Discharge would flow into the car rather than the grid.

## Architecture

All logic lives in `lambda_function.py`. Entry point is `lambda_handler(event, context)` (called by Lambda) or `main()` (for local runs).

### External APIs

| Service | Purpose | Auth |
|---|---|---|
| **GivEnergy** (`api.givenergy.cloud/v1`) | Battery status, settings, export control, push notifications | Bearer token |
| **Octopus Energy** (GraphQL at `api.octopus.energy/v1/graphql/`) | EV charging schedule (planned dispatches) | API key, exchanged for token via `obtainKrakenToken` mutation |
| **Solcast** (`api.solcast.com.au`) | Solar generation forecast (free tier: 9 requests/day, cached in S3) | API key in query param |
| **AWS S3** | Caches solar forecast JSON between invocations | boto3 (IAM role) |

### Key constants

- Battery: 5.22 kWh total, 80% usable depth of discharge, 2.6 kW discharge rate, 3.68 kW inverter max
- Tariff: Octopus Go, off-peak 23:30-05:30
- Solar forecast refresh: every 50 minutes during peak before 13:00

## Setup

### Environment variables

Copy `.env.example` to `.env` and fill in your values. See the example file for all required variables.

### Run locally

```bash
source .env && python lambda_function.py
```

### Deploy

```bash
./deploy.sh
```

This installs dependencies, zips them with `lambda_function.py`, and updates the Lambda function. Requires `lambda:UpdateFunctionCode` permission.

### Test

```bash
source .venv/bin/activate && pytest
```
