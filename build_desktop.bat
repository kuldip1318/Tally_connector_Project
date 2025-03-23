@echo off
echo Building Next.js application...
call npm run build

echo Packaging Electron application...
call npm run package

echo Build complete. The packaged application is in the dist folder.
pause

