# tg_tsuhan_updater_public

Spiders for a few Japanese shopping sites.

**\*This is a part of a larger private project. I only uploaded relevant files.
You may find functions unnecessary or not working.\***

> ### Disclaimer
> 
> I received tremendous help from telegram group and specificly,
> [marvinody](https://github.com/marvinody)'s project [mercari
> wrapper](https://github.com/marvinody/mercari/). 
> 
> Thanks marvinody, without your code an amateur like me would never crack those
> crypto and signature nonsense.

## What will this do

This will go through some Japanese online shops, looking for keywords you
specified, and return a html-style summary for any new, discounted, or sold /
bidding item.

Then, just find any online preview site and dump it in, enjoy : )

<br>

_**Currently support**: Yahoo Acution, Mercari, Lashinbang._

## How to use

**\*Checking all files and customizing on your own is strongly recommended.\***

I split these files from the original project in rather haste, and cannot
guarantee it would work as expected.

<br>

Still if you'd rather not do so, follow these steps to have a quick go:

> 1. Make sure you have installed Python 3 and dependencies (check those files to
>   see libs required). 3.8+ is recommended.
>
> 2. Open config.ini and setup accordingly.
>
> 3. Run some python command:
>
>```python
>from bot_autorun import main
>message = main()
>```
>
> 4. Copy the message out, or save it into a file, just do what you like.

## Debug Notice

Feel free to customize this project to fit your purpose. I suggest implement a
filter rule set, or set tasks and link the script with a messenger bot (e.g.
telegram bot).

Please refer to the license for detailed information regarding modification,
redistribution, warranty and responsibility.
