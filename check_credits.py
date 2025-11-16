"""
Program to check remaining QuickFS API credits for the day
"""
from quickfs import QuickFS
from datetime import datetime
import json
from config import QUICKFS_API_KEY

# QuickFS API Configuration
API_KEY = QUICKFS_API_KEY

def check_credits():
    """
    Check and display QuickFS API credit usage
    """
    try:
        print("Checking QuickFS API credits...")
        print("=" * 60)
        
        client = QuickFS(API_KEY)
        usage = client.get_usage()
        
        if not usage or "quota" not in usage:
            print("Error: Could not retrieve usage information")
            return
        
        quota = usage["quota"]
        used = quota.get("used", 0)
        remaining = quota.get("remaining", 0)
        resets_str = quota.get("resets", "")
        
        # Calculate total quota
        total = used + remaining
        
        # Parse reset time
        reset_time = None
        if resets_str:
            try:
                # Parse ISO format datetime
                reset_time = datetime.fromisoformat(resets_str.replace('Z', '+00:00'))
                # Convert to local time (remove timezone for display)
                reset_time = reset_time.replace(tzinfo=None)
            except:
                pass
        
        # Display information
        print(f"\nQuickFS API Credit Status")
        print(f"{'=' * 60}")
        print(f"Total Quota:     {total:,}")
        print(f"Credits Used:    {used:,}")
        print(f"Credits Remaining: {remaining:,}")
        
        # Calculate percentage
        if total > 0:
            used_pct = (used / total) * 100
            remaining_pct = (remaining / total) * 100
            print(f"\nUsage:           {used_pct:.1f}%")
            print(f"Remaining:       {remaining_pct:.1f}%")
        
        # Display reset time
        if reset_time:
            print(f"\nQuota Resets:    {reset_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f"                ({reset_time.strftime('%B %d, %Y at %I:%M %p')} UTC)")
        
        # Visual progress bar
        if total > 0:
            bar_length = 50
            used_bars = int((used / total) * bar_length)
            remaining_bars = bar_length - used_bars
            
            print(f"\nProgress Bar:")
            print(f"[{'#' * used_bars}{'.' * remaining_bars}]")
            print(f" {'Used':<10} {'Remaining':>10}")
        
        # Warning if low on credits
        if remaining < 100:
            print(f"\nWARNING: Low on credits! Only {remaining} remaining.")
        elif remaining < 1000:
            print(f"\nNote: {remaining} credits remaining. Consider monitoring usage.")
        else:
            print(f"\nYou have plenty of credits remaining.")
        
        print(f"\n{'=' * 60}")
        
        return usage
        
    except Exception as e:
        print(f"Error checking credits: {e}")
        return None

def main():
    """
    Main function to check QuickFS credits
    """
    check_credits()

if __name__ == "__main__":
    main()

